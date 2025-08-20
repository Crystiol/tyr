// io_uring_server_zero_copy.cpp
// 单文件 io_uring 高性能服务器（双层 BufferPool + 异步 accept/read/write + worker pool）
// - 依赖 liburing (liburing.h)；编译：g++ -std=c++17 -O2 io_uring_server_zero_copy.cpp -luring -lpthread -o iouring_server
// - 设计要点：
//    * accept/read/write 全部通过 io_uring 异步提交（减少 syscalls，低延迟）
//    * 双层 BufferPool（SMALL / LARGE）避免频繁 malloc，支持引用计数用于“共享/回写”
//    * 简易 RingBuffer 风格数据处理（这里用 steal 模式将 BufferBlock 直接传给 worker）
//    * WorkerPool 负责业务（示例 echo），处理完成后通过 io_uring 提交 send/write
//    * 使用 user_data 指针区分不同请求类型（ACCEPT/RECV/SEND/TIMER）
//    * metrics 简单输出到 stderr
//
// 该实现为示例性生产框架：真实生产会加入更健壮的错误处理、backpressure、connection limits、buffer registration（固定缓冲区 + register_buffers）等。

#include <liburing.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <sys/eventfd.h>
#include <sys/resource.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <unordered_map>
#include <queue>

using namespace std::chrono;
using namespace std;

// ------------------------- 配置 -------------------------
static const size_t SMALL_BLOCK = 512;
static const size_t LARGE_BLOCK = 4096;
static const size_t OUT_RING_CAP = 1024;
static const int MAX_ACCEPT_BATCH = 128;
static const uint64_t DEFAULT_IDLE_MS = 60 * 1000;
static const unsigned QUEUE_DEPTH = 4096;
static const int BACKLOG = 4096;

// ------------------------- 日志 / 工具 -------------------------
static inline uint64_t now_ms() { return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count(); }
#define LOGI(fmt, ...) fprintf(stdout, "[I] " fmt "\n", ##__VA_ARGS__)
#define LOGE(fmt, ...) fprintf(stderr, "[E] " fmt "\n", ##__VA_ARGS__)

// ------------------------- Metrics -------------------------
struct Metrics {
    std::atomic<uint64_t> accepted{0}, closed{0}, rx_bytes{0}, tx_bytes{0}, rx_pkts{0}, tx_pkts{0}, drops{0}, parse_errors{0}, timeouts{0};
} g_metrics;

static string metrics_text() {
    char b[512];
    int n = snprintf(b, sizeof b,
                     "accepted %llu\nclosed %llu\nrx_bytes %llu\ntx_bytes %llu\nrx_pkts %llu\ntx_pkts %llu\ndrops %llu\nparse_errors %llu\ntimeouts %llu\n",
                     (unsigned long long)g_metrics.accepted.load(),
                     (unsigned long long)g_metrics.closed.load(),
                     (unsigned long long)g_metrics.rx_bytes.load(),
                     (unsigned long long)g_metrics.tx_bytes.load(),
                     (unsigned long long)g_metrics.rx_pkts.load(),
                     (unsigned long long)g_metrics.tx_pkts.load(),
                     (unsigned long long)g_metrics.drops.load(),
                     (unsigned long long)g_metrics.parse_errors.load(),
                     (unsigned long long)g_metrics.timeouts.load());
    return string(b, n);
}

// ------------------------- BufferPool（双层） -------------------------
struct BufferBlock {
    std::atomic<int> ref{0};
    BufferBlock* next{nullptr};
    size_t cap{0};
    uint8_t* data{nullptr};
};

class BufferPoolBase {
public:
    BufferPoolBase(size_t bsize, size_t nblocks) : block_size_(bsize), blocks_(nblocks) {
        storage_.resize(blocks_);
        backing_.resize(blocks_ * block_size_);
        for (size_t i = 0; i < blocks_; ++i) {
            storage_[i].cap = block_size_;
            storage_[i].data = backing_.data() + i * block_size_;
            storage_[i].ref.store(0, memory_order_relaxed);
            storage_[i].next = (i + 1 < blocks_) ? &storage_[i + 1] : nullptr;
        }
        freelist_.store(&storage_[0], memory_order_release);
    }
    BufferBlock* acquire() {
        BufferBlock* h = freelist_.load(memory_order_acquire);
        while (h) {
            BufferBlock* nxt = h->next;
            if (freelist_.compare_exchange_weak(h, nxt, memory_order_acq_rel)) {
                h->next = nullptr;
                h->ref.store(1, memory_order_release);
                return h;
            }
        }
        return nullptr;
    }
    void retain(BufferBlock* b) { b->ref.fetch_add(1, memory_order_acq_rel); }
    void release(BufferBlock* b) {
        int prev = b->ref.fetch_sub(1, memory_order_acq_rel);
        if (prev == 1) {
            BufferBlock* head = freelist_.load(memory_order_relaxed);
            do { b->next = head; } while (!freelist_.compare_exchange_weak(head, b, memory_order_release, memory_order_relaxed));
        }
    }
    size_t block_size() const { return block_size_; }
private:
    size_t block_size_;
    size_t blocks_;
    std::vector<BufferBlock> storage_;
    std::vector<uint8_t> backing_;
    std::atomic<BufferBlock*> freelist_;
};

class DualBufferPool {
public:
    DualBufferPool(size_t small_blocks, size_t large_blocks) : small_(SMALL_BLOCK, small_blocks), large_(LARGE_BLOCK, large_blocks) {}
    BufferBlock* acquire(size_t expect) {
        if (expect <= small_.block_size()) {
            if (auto b = small_.acquire()) return b;
        }
        return large_.acquire();
    }
    void retain(BufferBlock* b) { (b->cap == SMALL_BLOCK ? small_ : large_).retain(b); }
    void release(BufferBlock* b) { (b->cap == SMALL_BLOCK ? small_ : large_).release(b); }
private:
    BufferPoolBase small_;
    BufferPoolBase large_;
};

// ------------------------- 请求类型 & 上下文 -------------------------
enum ReqType : uint8_t { REQ_ACCEPT, REQ_RECV, REQ_SEND, REQ_TIMEOUT };

struct IoRequest {
    ReqType type;
    int fd;                      // sock fd for RECV/SEND; listen fd for ACCEPT
    BufferBlock* blk;            // buffer to read into / send from
    size_t len;                  // length read / to send
    sockaddr_in peer;            // for accept (optional)
    socklen_t peer_len;
    // for send: we reuse blk + offset info in app code; keep simple here
    IoRequest(ReqType t=REQ_ACCEPT): type(t), fd(-1), blk(nullptr), len(0), peer_len(0) {}
};

// ------------------------- Simple WorkerPool -------------------------
class WorkerPool {
public:
    using Job = function<void()>;
    WorkerPool(int n=4): stop_(false) {
        for (int i=0;i<n;++i) threads_.emplace_back([this]{ this->run(); });
    }
    ~WorkerPool() {
        stop_ = true;
        for (auto &t: threads_) if (t.joinable()) t.join();
    }
    void submit(Job j) {
        {
            lock_guard<mutex> lk(m_);
            q_.push(move(j));
        }
        cv_.notify_one();
    }
private:
    void run() {
        while (!stop_) {
            Job job;
            {
                unique_lock<mutex> lk(m_);
                cv_.wait_for(lk, chrono::milliseconds(50), [&]{ return stop_ || !q_.empty(); });
                if (stop_) break;
                if (q_.empty()) continue;
                job = move(q_.front()); q_.pop();
            }
            if (job) job();
        }
    }
    vector<thread> threads_;
    queue<Job> q_;
    mutex m_;
    condition_variable cv_;
    atomic<bool> stop_;
};

// ------------------------- Connection 表（简化） -------------------------
struct Connection {
    int fd;
    uint64_t last_active_ms;
    // 出站简单队列：保存 BufferBlock 指针和长度（示例）
    vector<pair<BufferBlock*, size_t>> outq;
    mutex out_mtx;
    Connection(int f= -1): fd(f), last_active_ms(now_ms()) {}
};
using ConnPtr = shared_ptr<Connection>;

// ------------------------- 全局结构 -------------------------
struct IoUringServer {
    int listen_fd{-1};
    struct io_uring ring;
    DualBufferPool pool;
    WorkerPool workers;
    unordered_map<int, ConnPtr> conns; // protected by mutex_conns for simplicity
    mutex mutex_conns;
    atomic<bool> stop{false};
    int port;
    int metrics_port;
    IoUringServer(int p, int mp, size_t small_blocks, size_t large_blocks, int nworkers)
      : pool(small_blocks, large_blocks), workers(nworkers), port(p), metrics_port(mp) {
        memset(&ring, 0, sizeof ring);
    }
    ~IoUringServer() { io_uring_queue_exit(&ring); if (listen_fd >=0) close(listen_fd); }
};

// ------------------------- helpers -------------------------
static inline void set_socket_opts(int fd) {
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
}

// ------------------------- submit helpers -------------------------
static inline IoRequest* alloc_req(ReqType t) {
    IoRequest* r = (IoRequest*)malloc(sizeof(IoRequest));
    new (r) IoRequest(t);
    return r;
}

// 用 io_uring 提交 accept
void submit_accept(IoUringServer &S) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&S.ring);
    if (!sqe) { LOGE("no sqe for accept"); return; }
    IoRequest *req = alloc_req(REQ_ACCEPT);
    req->fd = S.listen_fd;
    req->peer_len = sizeof(req->peer);
    io_uring_prep_accept(sqe, S.listen_fd, (struct sockaddr*)&req->peer, &req->peer_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    io_uring_sqe_set_data(sqe, req);
    int rc = io_uring_submit(&S.ring);
    if (rc < 0) LOGE("io_uring_submit accept rc=%d", rc);
}

// 用 io_uring 提交 recv 到 pool 分配的块（一次只读到一个块）
void submit_recv(IoUringServer &S, int cfd) {
    BufferBlock* b = S.pool.acquire(S.pool.block_size()); // expect small by default
    if (!b) { LOGE("no buffer for recv"); return; }
    struct io_uring_sqe *sqe = io_uring_get_sqe(&S.ring);
    if (!sqe) { S.pool.release(b); LOGE("no sqe for recv"); return; }
    IoRequest* req = alloc_req(REQ_RECV);
    req->fd = cfd; req->blk = b; req->len = b->cap;
    io_uring_prep_recv(sqe, cfd, b->data, b->cap, 0);
    io_uring_sqe_set_data(sqe, req);
    int rc = io_uring_submit(&S.ring);
    if (rc < 0) { S.pool.release(b); LOGE("io_uring_submit recv rc=%d", rc); }
}

// 用 io_uring 提交 send（发送 out buffer）
void submit_send(IoUringServer &S, int cfd, BufferBlock* b, size_t len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&S.ring);
    if (!sqe) { S.pool.release(b); LOGE("no sqe for send"); return; }
    IoRequest* req = alloc_req(REQ_SEND);
    req->fd = cfd; req->blk = b; req->len = len;
    io_uring_prep_send(sqe, cfd, b->data, len, 0);
    io_uring_sqe_set_data(sqe, req);
    int rc = io_uring_submit(&S.ring);
    if (rc < 0) { S.pool.release(b); LOGE("io_uring_submit send rc=%d", rc); }
}

// ------------------------- business (示例 echo) -------------------------
void handle_business_echo(IoUringServer &S, int cfd, BufferBlock* b, size_t len) {
    // echo: 把 recv 到的 buffer 直接发送回去（零拷贝思想）
    // 将 buffer 的引用交给 send，send 完成会 release buffer
    // Submit send via io_uring
    // For concurrency safety we ensure connection exists
    {
        lock_guard<mutex> lk(S.mutex_conns);
        if (!S.conns.count(cfd)) { // connection closed meanwhile
            S.pool.release(b);
            return;
        }
    }
    g_metrics.rx_pkts++; g_metrics.rx_bytes += len;
    // For demo: submit send directly. In production, might queue outbound if concurrent sends exist.
    submit_send(S, cfd, b, len);
}

// ------------------------- completion handling -------------------------
void process_cqe(IoUringServer &S, struct io_uring_cqe *cqe) {
    IoRequest* req = (IoRequest*)io_uring_cqe_get_data(cqe);
    int res = cqe->res;
    io_uring_cqe_seen(&S.ring, cqe); // mark seen; (we still own req pointer)
    if (!req) { LOGE("null req in cqe"); return; }
    switch (req->type) {
        case REQ_ACCEPT: {
            if (res < 0) {
                if (res == -EAGAIN || res == -EWOULDBLOCK) {
                    // nothing; retry accept
                } else {
                    LOGE("accept failed: %s", strerror(-res));
                }
                // re-submit accept
                free(req);
                submit_accept(S);
                break;
            }
            int cfd = res;
            // set opts
            set_socket_opts(cfd);
            {
                // register connection
                lock_guard<mutex> lk(S.mutex_conns);
                auto conn = make_shared<Connection>(cfd);
                S.conns[cfd] = conn;
            }
            g_metrics.accepted++;
            // post initial recv on new connection
            submit_recv(S, cfd);
            // re-submit accept for next connection (keep accept queue full)
            free(req);
            submit_accept(S);
            break;
        }
        case REQ_RECV: {
            int cfd = req->fd;
            BufferBlock* b = req->blk;
            if (res <= 0) {
                if (res == 0 || res == -ECONNRESET || res == -ENOTCONN) {
                    // connection closed
                    lock_guard<mutex> lk(S.mutex_conns);
                    if (S.conns.count(cfd)) { S.conns.erase(cfd); g_metrics.closed++; }
                    if (b) S.pool.release(b);
                } else {
                    // EAGAIN or other error: try another recv
                    if (b) S.pool.release(b); // release reused buffer
                }
                free(req);
                break;
            }
            size_t got = (size_t)res;
            // For this sample we treat the buffer as a complete packet (protocol parsing omitted)
            // Submit to worker
            S.workers.submit([&S, cfd, b, got](){ handle_business_echo(S, cfd, b, got); });
            // post another recv for this connection
            free(req);
            submit_recv(S, cfd);
            break;
        }
        case REQ_SEND: {
            int cfd = req->fd;
            BufferBlock* b = req->blk;
            if (res < 0) {
                LOGE("send error on fd %d: %s", cfd, strerror(-res));
                // close connection
                lock_guard<mutex> lk(S.mutex_conns);
                if (S.conns.count(cfd)) { S.conns.erase(cfd); g_metrics.closed++; }
                if (b) S.pool.release(b);
                free(req);
                break;
            }
            size_t sent = (size_t)res;
            g_metrics.tx_bytes += sent;
            g_metrics.tx_pkts++;
            // release buffer after send
            if (b) S.pool.release(b);
            free(req);
            break;
        }
        default:
            LOGE("unknown req type");
            free(req);
            break;
    }
}

// ------------------------- server init / run -------------------------
int create_and_bind(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { LOGE("socket fail: %s", strerror(errno)); return -1; }
    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &on, sizeof(on));
#endif
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(fd, (sockaddr*)&addr, sizeof addr) < 0) { LOGE("bind fail: %s", strerror(errno)); close(fd); return -1; }
    if (listen(fd, BACKLOG) < 0) { LOGE("listen fail: %s", strerror(errno)); close(fd); return -1; }
    fcntl(fd, F_SETFL, fcntl(fd, F_GETFL, 0) | O_NONBLOCK);
    return fd;
}

void run_server(IoUringServer &S) {
    // init ring
    struct io_uring_params params;
    memset(&params, 0, sizeof params);
    if (io_uring_queue_init_params(QUEUE_DEPTH, &S.ring, &params) < 0) die("io_uring_queue_init_params");

    // Submit initial accept(s)
    for (int i=0;i<4;++i) submit_accept(S);

    // Completion loop
    struct io_uring_cqe *cqe;
    while (!S.stop.load()) {
        int ret = io_uring_wait_cqe_timeout(&S.ring, &cqe, nullptr);
        if (ret == -ETIME) {
            // timeout - can use for stats or timers
            continue;
        } else if (ret < 0) {
            if (ret == -EINTR) continue;
            LOGE("io_uring_wait_cqe_timeout ret=%d", ret);
            break;
        }
        if (cqe) {
            process_cqe(S, cqe);
            // NOTE: process_cqe calls io_uring_cqe_seen inside
        }
    }
}

// ------------------------- main -------------------------
static atomic<bool> g_terminate{false};
static void sigint_handler(int s){ LOGI("signal %d", s); g_terminate = true; }

int main(int argc, char** argv) {
    int port = 9000;
    int metrics_port = 9100;
    if (argc > 1) port = atoi(argv[1]);
    if (argc > 2) metrics_port = atoi(argv[2]);

    signal(SIGINT, sigint_handler);
    signal(SIGTERM, sigint_handler);

    int ncpu = get_nprocs();
    size_t small_blocks = (size_t)ncpu * 32 * 1024;
    size_t large_blocks = (size_t)ncpu * 16 * 1024;
    int nworkers = max(1, ncpu);

    LOGI("Starting io_uring server on port %d (workers=%d)", port, nworkers);

    IoUringServer S(port, metrics_port, small_blocks, large_blocks, nworkers);

    S.listen_fd = create_and_bind(port);
    if (S.listen_fd < 0) return 1;

    // Run metrics thread for visibility
    thread metrics_thr([&](){
        uint64_t last_rx=0, last_tx=0;
        auto last = steady_clock::now();
        while (!g_terminate.load()) {
            this_thread::sleep_for(chrono::seconds(1));
            auto now = steady_clock::now();
            double s = duration_cast<duration<double>>(now-last).count();
            last = now;
            uint64_t rx = g_metrics.rx_bytes.load(), tx = g_metrics.tx_bytes.load();
            double rxrate = (rx - last_rx) / s, txrate = (tx - last_tx) / s;
            last_rx = rx; last_tx = tx;
            LOGI("metrics acc=%llu cls=%llu rx=%llu tx=%llu rx/s=%.0fB tx/s=%.0fB pkts(rx=%llu tx=%llu) drop=%llu err=%llu to=%llu",
                 (unsigned long long)g_metrics.accepted.load(), (unsigned long long)g_metrics.closed.load(),
                 (unsigned long long)rx, (unsigned long long)tx, rxrate, txrate,
                 (unsigned long long)g_metrics.rx_pkts.load(), (unsigned long long)g_metrics.tx_pkts.load(),
                 (unsigned long long)g_metrics.drops.load(), (unsigned long long)g_metrics.parse_errors.load(),
                 (unsigned long long)g_metrics.timeouts.load());
        }
    });

    // Run server loop on main thread
    run_server(S);

    // shutdown
    S.stop = true;
    if (metrics_thr.joinable()) metrics_thr.join();
    LOGI("server exiting");
    return 0;
}
