// epoll_server_prod_zero_copy_et_opt.cpp
// 单文件高性能零拷贝 Epoll 服务器（全 ET / 批量 accept / 双层 BufferPool）
// - 完整实现：BufferPool / RingBuffer / Connection / Reactor / WorkerPool / TimingWheel / metrics
// - 关键位置已标注 [ET-CRITICAL]
// - 修正：保留 business_worker_echo 原始签名，添加 ConnectionPool 和 RingBufferPool 对象池，
//         将 Connection::out_ring 从 std::vector<OutEntry> 改为单一 BufferBlock*，优化大包并减少碎片
// 编译：g++ -std=c++11 -O2 epoll_server_prod_zero_copy_et_opt.cpp -lpthread -o server

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <new>
#include <sched.h>
#include <string>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/sysinfo.h>
#include <sys/timerfd.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include <sys/time.h>
#include <time.h>

#include <boost/asio.hpp>
#include <boost/asio/steady_timer.hpp>

using namespace std::chrono;
using namespace boost::asio;
using namespace boost::system;
using tcp = ip::tcp;

// -------------------------- 基础工具 & 日志 --------------------------
static uint64_t now_ms() {
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

#define LOG_INFO(fmt, ...) fprintf(stdout, "[INFO] " fmt "\n", ##__VA_ARGS__)
#define LOG_ERROR(fmt, ...) fprintf(stderr, "[ERROR] " fmt "\n", ##__VA_ARGS__)

static inline void die(const char *s) {
    perror(s);
    exit(1);
}

static inline int set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0)
        return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static inline int set_tcp_options(int fd) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
    return 0;
}

// -------------------------- Config --------------------------
static const size_t SMALL_BLOCK = 512; // 小块：协议头或小包
static const size_t LARGE_BLOCK = 4096; // 大块：主体或大包
static const int MAX_ACCEPT_BATCH = 64; // 每次 accept 最多尝试次数
static const size_t OUT_BUFFER_CAP = LARGE_BLOCK; // 出站缓冲区容量（单一 BufferBlock）
static const uint64_t DEFAULT_IDLE_MS = 60 * 1000;
static const uint64_t DEFAULT_ACTIVE_MS = 5 * 60 * 1000;

// -------------------------- Metrics --------------------------
struct Metrics {
    std::atomic<uint64_t> accepted{0}, closed{0}, rx_bytes{0}, tx_bytes{0},
        rx_pkts{0}, tx_pkts{0}, drops{0}, parse_errors{0}, timeouts{0}, s_pool{0}, l_pool{0}, r_pool{0}, c_pool{0};
} g_metrics;

static std::string metrics_text() {
    char buf[1024];
    int n = snprintf(buf, sizeof(buf),
                     "server_accepted %llu\n"
                     "server_closed %llu\n"
                     "server_rx_bytes %llu\n"
                     "server_tx_bytes %llu\n"
                     "server_rx_pkts %llu\n"
                     "server_tx_pkts %llu\n"
                     "server_drops %llu\n"
                     "server_parse_errors %llu\n"
                     "server_timeouts %llu\n"
                     "small_pool %llu\n"
                     "large_pool %llu\n"
                     "ring_pool %llu\n"
                     "conn_pool %llu\n",
                     (unsigned long long)g_metrics.accepted.load(),
                     (unsigned long long)g_metrics.closed.load(),
                     (unsigned long long)g_metrics.rx_bytes.load(),
                     (unsigned long long)g_metrics.tx_bytes.load(),
                     (unsigned long long)g_metrics.rx_pkts.load(),
                     (unsigned long long)g_metrics.tx_pkts.load(),
                     (unsigned long long)g_metrics.drops.load(),
                     (unsigned long long)g_metrics.parse_errors.load(),
                     (unsigned long long)g_metrics.timeouts.load(),
                     (unsigned long long)g_metrics.s_pool.load(),
                     (unsigned long long)g_metrics.l_pool.load(),
                     (unsigned long long)g_metrics.r_pool.load(),
                     (unsigned long long)g_metrics.c_pool.load());
    return std::string(buf, n);
}

// -------------------------- CRC32（用于头校验示例） --------------------------
static inline uint32_t crc32_calc(const void *data, size_t len) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j)
                c = c & 1 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        init = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < len; ++i)
        c = table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

static inline uint32_t bswap32_u32(uint32_t v) { return __builtin_bswap32(v); }

// -------------------------- BufferBlock & 双层 BufferPool --------------------------
struct alignas(64) BufferBlock {
    std::atomic<int> refcount;
    BufferBlock *next;
    size_t cap;
    uint8_t *data;
};

class BufferPoolBase {
public:
    BufferPoolBase(size_t block_size, size_t total_blocks) : bs_(block_size) {
        storage_.reserve(total_blocks);
        backing_.reserve(total_blocks * block_size);
        for (size_t i = 0; i < total_blocks; ++i){
            storage_.emplace_back(std::make_unique<BufferBlock>());
        }

        for (size_t i = 0; i < total_blocks; ++i) {
            storage_[i]->cap = bs_;
            storage_[i]->data = &backing_[i * bs_];
            storage_[i]->refcount.store(0, std::memory_order_relaxed);
            storage_[i]->next = (i + 1 < total_blocks ? storage_[i + 1].get() : nullptr);
        }
        freelist_.store(storage_[0].get(), std::memory_order_release);
    }

    BufferBlock *acquire() {
        BufferBlock *h = freelist_.load(std::memory_order_acquire);
        while (h) {
            BufferBlock *nxt = h->next;
            if (freelist_.compare_exchange_weak(h, nxt, std::memory_order_acq_rel)) {
                h->next = nullptr;
                h->refcount.store(1, std::memory_order_release);
                return h;
            }
        }
        return nullptr;
    }

    void retain(BufferBlock *b) {
        b->refcount.fetch_add(1, std::memory_order_acq_rel);
    }

    bool release_ref(BufferBlock *b) {
        int prev = b->refcount.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) {
            BufferBlock *head = freelist_.load(std::memory_order_relaxed);
            do {
                b->next = head;
            } while (!freelist_.compare_exchange_weak(
                head, b, std::memory_order_release, std::memory_order_relaxed));
            return true;
        }
        return false;
    }

    size_t block_size() const { return bs_; }

private:
    size_t bs_;
    std::vector<std::unique_ptr<BufferBlock>> storage_;
    std::vector<uint8_t> backing_;
    std::atomic<BufferBlock *> freelist_;
};

class DualBufferPool {
public:
    DualBufferPool(size_t small_blocks, size_t large_blocks)
        : small_(SMALL_BLOCK, small_blocks), large_(LARGE_BLOCK, large_blocks) {
        g_metrics.s_pool.store(small_blocks);
        g_metrics.l_pool.store(large_blocks);
    }

    BufferBlock *acquire(size_t expect) {
        if (expect <= small_.block_size()) {
            if (auto b = small_.acquire()){
                g_metrics.s_pool.fetch_sub(1, std::memory_order_relaxed);
                return b;
            }
        }
        g_metrics.l_pool.fetch_sub(1, std::memory_order_relaxed);
        return large_.acquire();
    }

    void retain(BufferBlock *b) {
        (b->cap == SMALL_BLOCK ? small_ : large_).retain(b);
    }

    void release_ref(BufferBlock *b) {
        bool bRet = (b->cap == SMALL_BLOCK ? small_ : large_).release_ref(b);
        if (bRet)
            b->cap == SMALL_BLOCK ? g_metrics.s_pool.fetch_add(1, std::memory_order_relaxed) : g_metrics.l_pool.fetch_add(1, std::memory_order_relaxed);
    }

private:
    BufferPoolBase small_;
    BufferPoolBase large_;
};

// -------------------------- MPMC 环形队列（用于工作队列 / timingwheel） --------------------------
template<typename T>
class MPMCRing {
public:
    explicit MPMCRing(size_t cap_pow2) : size_(cap_pow2), mask_(cap_pow2 - 1) {
        assert((cap_pow2 & (cap_pow2 - 1)) == 0);
        entries_.reserve(size_);
        for (size_t i = 0; i < size_; ++i) {
            entries_.emplace_back(std::make_unique<Entry>(i));
        }
        head_.store(0);
        tail_.store(0);
    }

    bool enqueue(const T &v) {
        Entry *e;
        size_t pos = head_.load(std::memory_order_relaxed);
        for (;;) {
            e = entries_[pos & mask_].get();
            size_t seq = e->seq.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)pos;
            if (dif == 0) {
                if (head_.compare_exchange_weak(pos, pos + 1)) {
                    e->val = v;
                    e->seq.store(pos + 1, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = head_.load(std::memory_order_relaxed);
            }
        }
    }

    bool dequeue(T &out) {
        Entry *e;
        size_t pos = tail_.load(std::memory_order_relaxed);
        for (;;) {
            e = entries_[pos & mask_].get();
            size_t seq = e->seq.load(std::memory_order_acquire);
            intptr_t dif = (intptr_t)seq - (intptr_t)(pos + 1);
            if (dif == 0) {
                if (tail_.compare_exchange_weak(pos, pos + 1)) {
                    out = e->val;
                    e->seq.store(pos + size_, std::memory_order_release);
                    return true;
                }
            } else if (dif < 0) {
                return false;
            } else {
                pos = tail_.load(std::memory_order_relaxed);
            }
        }
    }

private:
    struct Entry {
        std::atomic<size_t> seq;
        T val;

        Entry(size_t s) : seq(s), val() {}
    };

    std::vector<std::unique_ptr<Entry>> entries_;
    size_t size_, mask_;
    std::atomic<size_t> head_, tail_;
};

template<typename T>
class WorkQueueWrapper {
public:
    WorkQueueWrapper() : q_(1 << 12) {}

    bool push(const T &v) { return q_.enqueue(v); }
    bool pop(T &o) { return q_.dequeue(o); }

private:
    MPMCRing<T> q_;
};

template<typename T>
using WorkQueue = WorkQueueWrapper<T>;

// -------------------------- 协议头（示例） --------------------------
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t magic;
    uint32_t body_len;
    uint8_t endian;
    uint32_t hdr_crc;
};
#pragma pack(pop)

// -------------------------- RingBuffer（零拷贝入站，支持双层块） --------------------------
class RingBuffer {
public:
    RingBuffer(DualBufferPool &pool)
        : pool_(pool), head_(nullptr), tail_(nullptr), head_off_(0), tail_off_(0) {
        append_block(SMALL_BLOCK);
    }

    ~RingBuffer() {
        BufferBlock *cur = head_;
        while (cur) {
            BufferBlock *nxt = cur->next;
            pool_.release_ref(cur);
            cur = nxt;
        }
        head_ = tail_ = nullptr;
    }

    void reset() {
        BufferBlock *cur = head_;
        while (cur) {
            BufferBlock *nxt = cur->next;
            pool_.release_ref(cur);
            cur = nxt;
        }
        head_ = tail_ = nullptr;
        head_off_ = tail_off_ = 0;
        append_block(SMALL_BLOCK);
    }

    // 获取可写区域（hint 参数决定分配小/大块）,写满一个块后才会写下一个扩展块
    iovec writable_region(size_t hint = SMALL_BLOCK) {
        ensure_tail(hint);
        return iovec{tail_->data + tail_off_, tail_->cap - tail_off_ - 1};
    }

    // 标记已写入 n 字节，根据需要扩展块
    void produce(size_t n) {
        tail_off_ += n;
        if (tail_off_ >= tail_->cap - 1) {
            append_block(SMALL_BLOCK);
            tail_off_ = 0;
        }
    }

    // peek 指定字节到 dst（不移动读指针）
    size_t peek_bytes(uint8_t *dst, size_t n) {
        if (!head_)
            return 0;
        size_t copied = 0;
        BufferBlock *cur = head_;
        size_t off = head_off_;
        size_t remain = n;
        while (remain > 0 && cur) {
            size_t avail = (cur == tail_) ? (tail_off_ - off) : (cur->cap - off);
            if (avail == 0)
                break;
            size_t take = std::min(avail, remain);
            memcpy(dst + copied, cur->data + off, take);
            copied += take;
            remain -= take;
            off += take;
            if (off >= cur->cap) {
                cur = cur->next;
                off = 0;
            }
        }
        return copied;
    }

    // 消耗 n 字节（推进读指针并释放完整块）
    void consume(size_t n) {
        size_t remain = n;
        while (remain > 0 && head_) {
            size_t avail = (head_ == tail_) ? (tail_off_ - head_off_) : (head_->cap - head_off_);
            if (avail > remain) {
                head_off_ += remain;
                return;
            }
            remain -= avail;
            BufferBlock *old = head_;
            head_ = old->next;          //head指向下一块
            pool_.release_ref(old);     //回收已读完的块
            head_off_ = 0;              //下一块的起始地址必定为0，因为还没有被cosume
            if (!head_) {
                tail_ = nullptr;
                tail_off_ = 0;
                break;
            }
        }
    }

    // 获取可读数据大小
    size_t readable_bytes() const {
        if (!head_)
            return 0;
        if (head_ == tail_) //只有一个块
            return (tail_off_ >= head_off_) ? (tail_off_ - head_off_) : 0;
        size_t cnt = 0;
        BufferBlock *cur = head_;
        size_t off = head_off_;
        while (cur) {       //多个块的情形，剩余空间+新块已用
            if (cur == tail_) {
                cnt += tail_off_ - off;
                break;
            }
            cnt += cur->cap - off;
            cur = cur->next;
            off = 0;
        }
        return cnt;
    }

    // 返回单一 BufferBlock，拷贝数据，按 body_len 选择小/大块
    BufferBlock *steal_body_after(size_t header_len, size_t body_len) {
        if (readable_bytes() < header_len + body_len)
            return nullptr;

        // 决定块大小
        size_t block_size = (body_len <= SMALL_BLOCK) ? SMALL_BLOCK : LARGE_BLOCK;
        BufferBlock *new_block = pool_.acquire(block_size);
        if (!new_block)
            return nullptr;

        // 推进到 header_len 后
        BufferBlock *cur = head_;
        size_t off = head_off_;
        size_t skip = header_len;
        while (skip > 0 && cur) {
            size_t avail = (cur == tail_) ? (tail_off_ - off) : (cur->cap - off);
            if (avail > skip) {
                off += skip;
                skip = 0;
                break;
            }
            skip -= avail;
            cur = cur->next;
            off = 0;
        }

        // 拷贝 body_len 到 new_block->data
        size_t remain = body_len;
        size_t copied = 0;
        while (remain > 0 && cur) {
            size_t avail = (cur == tail_) ? (tail_off_ - off) : (cur->cap - off);
            size_t take = std::min(avail, remain);
            memcpy(new_block->data + copied, cur->data + off, take);
            copied += take;
            remain -= take;
            off += take;
            if (off >= cur->cap) {
                cur = cur->next;
                off = 0;
            }
        }

        consume(header_len + body_len);
        return new_block;
    }

    std::atomic<int> refcount{0};
    RingBuffer *next = nullptr;

private:
    DualBufferPool &pool_;
    BufferBlock *head_;
    BufferBlock *tail_;
    size_t head_off_;
    size_t tail_off_;

    void append_block(size_t expect) {
        BufferBlock *b = pool_.acquire(expect);
        if (!b)
            die("BufferPool exhausted");
        b->next = nullptr;
        if (!head_) {
            head_ = tail_ = b;
            head_off_ = tail_off_ = 0;
        } else {
            tail_->next = b;        //将新块链接在当前块后边
            tail_ = b;
        }
    }

    void ensure_tail(size_t hint) {
        if (!tail_ || tail_off_ >= tail_->cap - 1) {
            append_block(hint);     //未分配块或块已满则分配新块
            tail_off_ = 0;
        }
    }
};

// -------------------------- RingBufferPool --------------------------
class RingBufferPool {
public:
    RingBufferPool(size_t total, DualBufferPool &dp) : dp_(dp) {
        storage_.resize(total);
        for (size_t i = 0; i < total; ++i) {
            storage_[i] = std::make_unique<RingBuffer>(dp_);
            storage_[i]->next = (i + 1 < total ? storage_[i + 1].get() : nullptr);
        }
        freelist_.store(storage_[0].get(), std::memory_order_release);
        g_metrics.r_pool.store(total);
    }

    RingBuffer *acquire() {
        RingBuffer *h = freelist_.load(std::memory_order_acquire);
        while (h) {
            RingBuffer *nxt = h->next;
            if (freelist_.compare_exchange_weak(h, nxt, std::memory_order_acq_rel)) {
                h->next = nullptr;
                h->refcount.store(1, std::memory_order_relaxed);
                g_metrics.r_pool.fetch_sub(1, std::memory_order_relaxed);
                return h;
            }
        }
        return nullptr;
    }

    void release_ref(RingBuffer *b) {
        int prev = b->refcount.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) {
            b->reset();
            RingBuffer *head = freelist_.load(std::memory_order_relaxed);
            do {
                b->next = head;
            } while (!freelist_.compare_exchange_weak(
                head, b, std::memory_order_release, std::memory_order_relaxed));
            g_metrics.r_pool.fetch_add(1, std::memory_order_relaxed);
        }
    }

private:
    DualBufferPool &dp_;
    std::vector<std::unique_ptr<RingBuffer>> storage_;
    std::atomic<RingBuffer *> freelist_;
};

// -------------------------- OutEntry & Connection --------------------------
struct OutEntry {
    BufferBlock *blk;
    size_t offset;
    size_t len;
};

class Connection {
public:
    std::atomic<int> refcount{0};
    Connection *next = nullptr;
    int fd = -1;
    RingBuffer *rx = nullptr;
    BufferBlock *out_buffer = nullptr; // 替换 out_ring 为单一 BufferBlock
    size_t out_offset = 0; // 当前写入偏移
    size_t out_len = 0; // 当前有效数据长度
    std::atomic<uint64_t> last_active_ms{0};
    std::shared_ptr<tcp::socket> socket_ = nullptr;

    Connection() {}

    void reset(DualBufferPool &p, RingBufferPool &rp) {
        if (fd >= 0) ::close(fd);
        fd = -1;
        if (rx) rp.release_ref(rx);
        rx = nullptr;
        if (out_buffer) {
            p.release_ref(out_buffer);
            out_buffer = nullptr;
        }
        out_offset = out_len = 0;
        last_active_ms = 0;
        if (socket_) {
            if (socket_->is_open()) socket_->close();
            socket_.reset();
        }
    }

    bool out_push(const OutEntry &e, DualBufferPool &pool) {
        if (!out_buffer) {
            out_buffer = pool.acquire(LARGE_BLOCK);     //分配发送缓冲区
            if (!out_buffer)
                return false;
        }
        if (out_offset + e.len > out_buffer->cap)
            return false; // 缓冲区不足
        memcpy(out_buffer->data + out_offset, e.blk->data + e.offset, e.len);
        out_offset += e.len;
        out_len += e.len;
        pool.retain(out_buffer);    //add发送缓冲区计数，可以避免再次分配
        return true;
    }

    bool out_empty() const { return out_len == 0; }

    iovec out_peek() {
        if (out_empty() || !out_buffer)
            return {nullptr, 0};
        return {out_buffer->data, out_len};
    }

    void out_advance(size_t bytes_written, DualBufferPool &pool) {
        if (bytes_written >= out_len) {
            out_len = 0;
            out_offset = 0;
            if (out_buffer) {
                pool.release_ref(out_buffer);            //回收发送缓冲区
                //out_buffer = nullptr;       //如果out_push中调用了pool.retain(out_buffer)，则不应该赋空
            }
        } else {
            out_len -= bytes_written;
            memmove(out_buffer->data, out_buffer->data + bytes_written, out_len);
            out_offset = out_len;
        }
    }
};

// -------------------------- ConnectionPool --------------------------
class ConnectionPool {
public:
    ConnectionPool(size_t max_conn, DualBufferPool &dp, RingBufferPool &rp)
        : dp_(dp), rp_(rp), max_active_(max_conn) {
        storage_.resize(max_conn);
        for (size_t i = 0; i < max_conn; ++i) {
            storage_[i] = std::make_unique<Connection>();
            storage_[i]->next = (i + 1 < max_conn ? storage_[i + 1].get() : nullptr);
        }
        freelist_.store(storage_[0].get(), std::memory_order_release);
        g_metrics.c_pool.store(max_conn);
    }

    bool admit(std::shared_ptr<tcp::socket> s, Connection **out_conn) {
        if (active_.load(std::memory_order_relaxed) >= max_active_)
            return false;

        Connection *conn = acquire();
        if (!conn)
            return false;

        RingBuffer *rb = rp_.acquire();
        if (!rb) {
            release_ref(conn);
            return false;
        }

        conn->socket_ = s;
        conn->fd = s->native_handle();
        conn->rx = rb;      //为连接分配接收缓冲区rx
        conn->last_active_ms.store(now_ms(), std::memory_order_relaxed);
        *out_conn = conn;
        return true;
    }

    void release_ref(Connection *c) {
        int prev = c->refcount.fetch_sub(1, std::memory_order_acq_rel);
        if (prev == 1) {
            c->reset(dp_, rp_);
            Connection *head = freelist_.load(std::memory_order_relaxed);
            do {
                c->next = head;
            } while (!freelist_.compare_exchange_weak(
                head, c, std::memory_order_release, std::memory_order_relaxed));
            active_.fetch_sub(1, std::memory_order_relaxed);
            g_metrics.c_pool.fetch_add(1, std::memory_order_relaxed);
        }
    }

private:
    Connection *acquire() {
        size_t cur = active_.load(std::memory_order_relaxed);
        while (cur < max_active_) {
            if (active_.compare_exchange_weak(cur, cur + 1, std::memory_order_acq_rel))
                break;
        }
        if (cur >= max_active_)
            return nullptr;

        Connection *h = freelist_.load(std::memory_order_acquire);
        while (h) {
            Connection *nxt = h->next;
            if (freelist_.compare_exchange_weak(h, nxt, std::memory_order_acq_rel)) {
                h->next = nullptr;
                h->refcount.store(1, std::memory_order_relaxed);
                g_metrics.c_pool.fetch_sub(1, std::memory_order_relaxed);
                return h;
            }
        }
        active_.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
    }

    DualBufferPool &dp_;
    RingBufferPool &rp_;
    size_t max_active_;
    std::vector<std::unique_ptr<Connection>> storage_;
    std::atomic<Connection *> freelist_;
    std::atomic<size_t> active_{0};
};

// -------------------------- TimingWheel --------------------------
class TimingWheel {
public:
    struct Entry {
        Connection *conn;
        uint64_t expire_ms;
    };

    explicit TimingWheel(uint64_t tick_ms, size_t slots)
        : tick_ms_(tick_ms), slots_(normalize_pow2(slots)), slot_mask_(slots_ - 1),
          initialized_(false), last_slot_index_(0) {
        slots_q_.reserve(slots_);
        for (size_t i = 0; i < slots_; ++i)
            slots_q_.emplace_back(std::make_unique<MPMCRing<Entry>>(1 << 14));
    }

    inline void add(Connection *conn, uint64_t expire_ms) {
        Entry e{conn, expire_ms};
        size_t idx = slot_index(expire_ms);
        for (int i = 0; i < 64; ++i) {
            if (slots_q_[idx]->enqueue(e))
                return;
            std::this_thread::yield();
        }
        g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
    }

    template<typename F>
    void tick(uint64_t now_ms, F on_timeout) {
        if (!initialized_.load(std::memory_order_acquire)) {
            size_t idx = (now_ms / tick_ms_) & slot_mask_;
            last_slot_index_.store(idx, std::memory_order_release);
            initialized_.store(true, std::memory_order_release);
            return;
        }
        size_t target = (now_ms / tick_ms_) & slot_mask_;
        size_t cur = last_slot_index_.load(std::memory_order_relaxed);
        while (cur != target) {
            drain(cur, now_ms, on_timeout);
            cur = (cur + 1) & slot_mask_;
        }
        last_slot_index_.store(cur, std::memory_order_relaxed);
        drain_light(cur, now_ms, on_timeout);
    }

private:
    template<typename F>
    void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        while (slots_q_[idx]->dequeue(e)) {
            if (e.expire_ms <= now_ms)
                on_timeout(e.conn);
            else
                add(e.conn, e.expire_ms);
        }
    }

    template<typename F>
    void drain_light(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        for (int k = 0; k < 64; ++k) {
            if (!slots_q_[idx]->dequeue(e))
                break;
            if (e.expire_ms <= now_ms)
                on_timeout(e.conn);
            else
                add(e.conn, e.expire_ms);
        }
    }

    static size_t normalize_pow2(size_t x) {
        size_t p = 1;
        while (p < x)
            p <<= 1;
        return p;
    }

    inline size_t slot_index(uint64_t expire_ms) const {
        return ((expire_ms / tick_ms_) & slot_mask_);
    }

    const uint64_t tick_ms_;
    const size_t slots_, slot_mask_;
    std::vector<std::unique_ptr<MPMCRing<Entry>>> slots_q_;
    std::atomic<bool> initialized_;
    std::atomic<size_t> last_slot_index_;
};

// -------------------------- TaskQueue / WorkerPool --------------------------
struct ResponseTask {
    int fd;
    OutEntry entry;
};

class TaskQueue {
public:
    TaskQueue() : q_(1 << 14) {}

    bool enqueue(const ResponseTask &t) { return q_.enqueue(t); }
    bool dequeue(ResponseTask &o) { return q_.dequeue(o); }

private:
    MPMCRing<ResponseTask> q_;
};

static inline void
business_worker_echo(int fd, BufferBlock *stolen_block, size_t body_len, TaskQueue &reactor_queue) {
    ResponseTask t;
    t.fd = fd;
    //memcpy(stolen_block->data, "aaaaaa", 6);
    //t.entry = {stolen_block, 0, 6};
    t.entry = {stolen_block, 0, body_len};
    while (!reactor_queue.enqueue(t))
        std::this_thread::yield();
}

class WorkerPool {
public:
    using Job = std::function<void()>;

    explicit WorkerPool(size_t n) : stop_(false) {
        for (size_t i = 0; i < n; ++i)
            threads_.emplace_back([this] { loop(); });
    }

    ~WorkerPool() {
        stop_.store(true);
        for (auto &t : threads_)
            t.join();
    }

    void submit(Job j) {
        while (!queue_push(j))
            std::this_thread::yield();
    }

private:
    bool queue_push(const Job &j) { return queue_.push(j); }

    void loop() {
        Job j;
        while (!stop_.load()) {
            if (queue_pop(j))
                j();
            else
                std::this_thread::yield();
        }
    }

    bool queue_pop(Job &j) { return queue_.pop(j); }
    std::vector<std::thread> threads_;
    std::atomic<bool> stop_;
    WorkQueue<Job> queue_;
};

// -------------------------- Reactor（ET 核心） --------------------------
class Reactor {
public:
    Reactor(int cpu_id, int listen_fd, DualBufferPool &pool,
            ConnectionPool &cpool, WorkerPool &workers, uint64_t idle_ms,
            uint64_t active_ms)
        : cpu_id_(cpu_id), listen_fd_(listen_fd), pool_(pool), cpool_(cpool),
          workers_(workers), idle_ms_(idle_ms), active_ms_(active_ms), taskq_(),
          wheel_(100, 1024) {
        timer_ = std::make_shared<steady_timer>(ioc_, milliseconds(100));
    }

    void run() {
        pin_cpu(cpu_id_);
        acceptor_ = std::make_shared<tcp::acceptor>(ioc_,tcp::endpoint(tcp::v4(),9000));
        /* 
        // metrics acceptor commented as in original
        metrics_acceptor_ = tcp::acceptor(ioc_, tcp::endpoint(ip::address::from_string("127.0.0.1"), metrics_port_));
        metrics_acceptor_.set_option(tcp::acceptor::reuse_address(true));
#ifdef SO_REUSEPORT
        // set reuse_port if needed
#endif
        */

        schedule_timer();
        start_accept();
        // start_metrics_accept(); // commented
        ioc_.run();
    }

    void stop() { 
        global_stop_.store(true);
        ioc_.stop(); 
    }
    void set_metrics_port(int p) { metrics_port_ = p; }
    bool enqueue_response(const ResponseTask &t) { return taskq_.enqueue(t); }

private:
    int cpu_id_;
    io_context ioc_;
    std::shared_ptr<tcp::acceptor> acceptor_;
    //std::shared_ptr<tcp::acceptor> metrics_acceptor_;
    std::shared_ptr<steady_timer> timer_;
    int listen_fd_;
    DualBufferPool &pool_;
    ConnectionPool &cpool_;
    WorkerPool &workers_;
    uint64_t idle_ms_, active_ms_;
    TaskQueue taskq_;
    TimingWheel wheel_;
    std::atomic<bool> global_stop_{false};
    int metrics_port_ = 9100;
    std::unordered_map<int, Connection *> conns_;

    static void pin_cpu(int cpu) {
#ifdef __linux__
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        sched_setaffinity(0, sizeof(set), &set);
#endif
    }

    void schedule_timer() {
        timer_->expires_after(milliseconds(100));
        timer_->async_wait([this](const error_code &ec) {
            if (ec || global_stop_.load()) return;
            uint64_t tnow = now_ms();
            wheel_.tick(tnow, [this](Connection *conn) { on_timeout(conn); });
            process_incoming_tasks(conns_);
            schedule_timer();
        });
    }

    void start_accept() {
        auto new_socket = std::make_shared<tcp::socket>(ioc_);
        acceptor_->async_accept(*new_socket, [this, new_socket](error_code ec) {
            if (ec) {
                if (!global_stop_.load()) start_accept();
                return;
            }
            do_accept(new_socket);
            start_accept();
        });
    }

    void do_accept(std::shared_ptr<tcp::socket> s) {
        int fd = s->native_handle();
        set_tcp_options(fd);
        Connection *conn = nullptr;
        if (!cpool_.admit(s, &conn)) {
            s->close();
            return;
        }
        conns_[fd] = conn;
        g_metrics.accepted.fetch_add(1, std::memory_order_relaxed);
        wheel_.add(conn, now_ms() + active_ms_);
        start_read(conn);
        while (true) {
            auto extra_socket = std::make_shared<tcp::socket>(ioc_);
            error_code ec;
            acceptor_->accept(*extra_socket, ec);
            if (ec) break;
            fd = extra_socket->native_handle();
            set_tcp_options(fd);
            Connection *extra_conn = nullptr;
            if (!cpool_.admit(extra_socket, &extra_conn)) {
                extra_socket->close();
                continue;
            }
            conns_[fd] = extra_conn;
            g_metrics.accepted.fetch_add(1, std::memory_order_relaxed);
            wheel_.add(extra_conn, now_ms() + active_ms_);
            start_read(extra_conn);
        }
    }

    void start_read(Connection *conn) {
        iovec w = conn->rx->writable_region(SMALL_BLOCK);
        if (w.iov_len == 0) return;
        conn->socket_->async_read_some(mutable_buffer(w.iov_base, w.iov_len),
                                       [this, conn](error_code ec, size_t bytes) {
                                           if (ec) {
                                               close_conn(conn, conns_);
                                               return;
                                           }
                                           conn->rx->produce(bytes);
                                           g_metrics.rx_bytes.fetch_add(bytes, std::memory_order_relaxed);
                                           conn->last_active_ms.store(now_ms(), std::memory_order_relaxed);
                                           while (true) {
                                               iovec w2 = conn->rx->writable_region(SMALL_BLOCK);
                                               error_code ec2;
                                               size_t b2 = conn->socket_->read_some(mutable_buffer(w2.iov_base, w2.iov_len), ec2);
                                               if (ec2 == error::would_block) break;
                                               if (ec2 || b2 == 0) {
                                                   close_conn(conn, conns_);
                                                   return;
                                               }
                                               conn->rx->produce(b2);
                                               g_metrics.rx_bytes.fetch_add(b2, std::memory_order_relaxed);
                                               conn->last_active_ms.store(now_ms(), std::memory_order_relaxed);
                                           }
                                           process_data(conn);
                                           start_read(conn);
                                       });
    }

    void process_data(Connection *conn) {
        const size_t H = 1 + 4 + 1 + 4;
        while (true) {
            if (conn->rx->readable_bytes() < H) break;
            uint8_t hdrbuf[10];
            size_t got = conn->rx->peek_bytes(hdrbuf, H);
            if (got < H) break;
            PacketHeader hdr;
            hdr.magic = hdrbuf[0];
            uint32_t bl;
            memcpy(&bl, hdrbuf + 1, 4);
            hdr.endian = hdrbuf[5];
            memcpy(&hdr.hdr_crc, hdrbuf + 6, 4);
            uint32_t crc = crc32_calc(hdrbuf, 6);
            if (hdr.magic != 0x7e || hdr.hdr_crc != crc) {
                g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
                close_conn(conn, conns_);
                return;
            }
            hdr.body_len = hdr.endian ? bswap32_u32(bl) : bl;
            const uint32_t MAX_BODY = 16 * 1024 * 1024;
            if (hdr.body_len > MAX_BODY) {
                g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
                close_conn(conn, conns_);
                return;
            }
            if (conn->rx->readable_bytes() < H + hdr.body_len) break;
            conn->rx->consume(H);
            (void)conn->rx->writable_region(std::min<size_t>(LARGE_BLOCK, std::max<size_t>(SMALL_BLOCK, hdr.body_len)));
            BufferBlock *stolen = conn->rx->steal_body_after(0, hdr.body_len);
            if (!stolen) {
                g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
                break;
            }
            g_metrics.rx_pkts.fetch_add(1, std::memory_order_relaxed);
            auto job = [fd = conn->fd, stolen, body_len = hdr.body_len, this]() {
                business_worker_echo(fd, stolen, body_len, taskq_);
            };
            workers_.submit(job);
        }
    }

    void start_write(Connection *conn) {
        if (conn->out_empty()) return;
        iovec ov = conn->out_peek();
        conn->socket_->async_write_some(const_buffer(ov.iov_base, ov.iov_len),
                                        [this, conn](error_code ec, size_t bytes) {
                                            if (ec) {
                                                close_conn(conn, conns_);
                                                return;
                                            }
                                            g_metrics.tx_bytes.fetch_add(bytes, std::memory_order_relaxed);
                                            g_metrics.tx_pkts.fetch_add(1, std::memory_order_relaxed);
                                            conn->out_advance(bytes, pool_);
                                            while (!conn->out_empty()) {
                                                iovec ov2 = conn->out_peek();
                                                error_code ec2;
                                                size_t b2 = conn->socket_->write_some(const_buffer(ov2.iov_base, ov2.iov_len), ec2);
                                                if (ec2 == error::would_block) break;
                                                if (ec2) {
                                                    close_conn(conn, conns_);
                                                    return;
                                                }
                                                g_metrics.tx_bytes.fetch_add(b2, std::memory_order_relaxed);
                                                g_metrics.tx_pkts.fetch_add(1, std::memory_order_relaxed);
                                                conn->out_advance(b2, pool_);
                                            }
                                            if (!conn->out_empty()) {
                                                start_write(conn);
                                            }
                                        });
    }

    void on_timeout(Connection *conn) {
        if (now_ms() - conn->last_active_ms.load(std::memory_order_relaxed) > idle_ms_) {
            g_metrics.timeouts.fetch_add(1, std::memory_order_relaxed);
            close_conn(conn, conns_);
        } else {
            wheel_.add(conn, now_ms() + active_ms_);
        }
    }

    void process_incoming_tasks(std::unordered_map<int, Connection *> &conns) {
        ResponseTask t;
        while (taskq_.dequeue(t)) {
            auto it = conns.find(t.fd);
            if (it == conns.end()) {
                pool_.release_ref(t.entry.blk);
                continue;
            }
            Connection *conn = it->second;
            bool was_empty = conn->out_empty();
            if (!conn->out_push(t.entry, pool_)) {
                g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
            } else if (was_empty) {
                start_write(conn);
            }
            pool_.release_ref(t.entry.blk);
        }
    }

    /* // commented as in original
    void start_metrics_accept() {
        auto new_sock = std::make_shared<tcp::socket>(ioc_);
        metrics_acceptor_.async_accept(*new_sock, [this, new_sock](error_code ec) {
            if (!ec) {
                serve_metrics(new_sock);
            }
            start_metrics_accept();
        });
    }

    void serve_metrics(std::shared_ptr<tcp::socket> sock) {
        do {
            char buf[1024];
            error_code ec;
            size_t n = sock->read_some(mutable_buffer(buf, sizeof(buf) - 1), ec);
            if (ec || n <= 0) {
                sock->close();
                break;
            }
            buf[n] = 0;
            if (strncmp(buf, "GET /metrics", 12) == 0) {
                std::string body = metrics_text();
                char hdr[256];
                int hn = snprintf(hdr, sizeof(hdr),
                                  "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: "
                                  "text/plain; version=0.0.4\r\nConnection: close\r\n\r\n",
                                  body.size());
                sock->write_some(const_buffer(hdr, hn), ec);
                sock->write_some(const_buffer(body.data(), body.size()), ec);
            } else {
                const char *r = "HTTP/1.1 404\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                sock->write_some(const_buffer(r, strlen(r)), ec);
            }
            sock->close();
            auto extra = std::make_shared<tcp::socket>(ioc_);
            error_code ec2;
            metrics_acceptor_.accept(*extra, ec2);
            if (ec2) break;
            sock = extra;
        } while (true);
    }
    */

    void close_conn(Connection *conn, std::unordered_map<int, Connection *> &conns) {
        if (!conn) return;
        g_metrics.closed.fetch_add(1, std::memory_order_relaxed);
        if (conn->out_buffer) {
            pool_.release_ref(conn->out_buffer);
            conn->out_buffer = nullptr;
        }
        conns.erase(conn->fd);
        cpool_.release_ref(conn);
    }
};

// -------------------------- 主程序 --------------------------
static std::atomic<bool> g_terminate{false};

static void signal_handler(int sig) {
    LOG_INFO("signal %d, terminating", sig);
    g_terminate.store(true);
}

int main(int argc, char **argv) {
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    uint16_t port = 9000;
    uint16_t metrics_port = 9100;
    if (argc > 1)
        port = (uint16_t)atoi(argv[1]);
    if (argc > 2)
        metrics_port = (uint16_t)atoi(argv[2]);

    int ncpu = get_nprocs();
    size_t small_blocks = (size_t)ncpu * 1000 * 1000; // 可按内存和连接数调节
    size_t large_blocks = (size_t)ncpu * 32 * 1000;

    LOG_INFO("ET-opt server starting on port %u with %d CPUs, small_blocks=%zu, "
             "large_blocks=%zu",
             port, ncpu, small_blocks, large_blocks);

    DualBufferPool pool(small_blocks, large_blocks);
    size_t max_conn = 100000;
    RingBufferPool rpool(max_conn, pool);
    ConnectionPool cpool(max_conn, pool, rpool);
    size_t worker_threads = std::max(1, ncpu * 1);
    WorkerPool workers(worker_threads);

    int listen_fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd < 0)
        die("socket");
    set_tcp_options(listen_fd);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(listen_fd, (sockaddr *)&addr, sizeof addr) < 0)
        die("bind");
    if (listen(listen_fd, 1024) < 0)
        die("listen");

    // 启动 per-cpu Reactor（共享同一个 listen_fd）
    std::vector<std::thread> reactor_threads;
    std::vector<std::unique_ptr<Reactor>> reactors;
    for (int i = 0; i < ncpu; ++i) {
        reactors.emplace_back(new Reactor(i, listen_fd, pool, cpool, workers,
                                         DEFAULT_IDLE_MS, DEFAULT_ACTIVE_MS));
        reactors.back()->set_metrics_port(metrics_port);
        reactor_threads.emplace_back([&r = reactors.back()]() { r->run(); });
    }

    std::thread metrics_printer([&] {
        uint64_t last_rx = 0, last_tx = 0;
        auto last = steady_clock::now();
        while (!g_terminate.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
            auto now = steady_clock::now();
            double s = duration_cast<duration<double>>(now - last).count();
            last = now;
            uint64_t rx = g_metrics.rx_bytes.load(), tx = g_metrics.tx_bytes.load();
            double rxrate = (rx - last_rx) / s, txrate = (tx - last_tx) / s;
            last_rx = rx;
            last_tx = tx;
            struct timeval tv;
            gettimeofday(&tv, NULL);

            // 转换为本地时间
            struct tm tm_info;
            localtime_r(&tv.tv_sec, &tm_info);

            fprintf(stderr,
                    "[metrics %04d-%02d-%02d %02d:%02d:%02d.%03ld] acc=%llu cls=%llu rx=%llu tx=%llu rx/s=%.0fB "
                    "tx/s=%.0fB pkts(rx=%llu tx=%llu) drop=%llu err=%llu to=%llu sp=%llu lp=%llu rp=%llu cp=%llu\n",
                    tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, tv.tv_usec / 1000,
                    (unsigned long long)g_metrics.accepted.load(),
                    (unsigned long long)g_metrics.closed.load(),
                    (unsigned long long)rx, (unsigned long long)tx, rxrate, txrate,
                    (unsigned long long)g_metrics.rx_pkts.load(),
                    (unsigned long long)g_metrics.tx_pkts.load(),
                    (unsigned long long)g_metrics.drops.load(),
                    (unsigned long long)g_metrics.parse_errors.load(),
                    (unsigned long long)g_metrics.timeouts.load(),
                    (unsigned long long)g_metrics.s_pool.load(),
                    (unsigned long long)g_metrics.l_pool.load(),
                    (unsigned long long)g_metrics.r_pool.load(),
                    (unsigned long long)g_metrics.c_pool.load());
        }
    });

    while (!g_terminate.load())
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    LOG_INFO("shutdown requested, stopping reactors");
    for (auto &r : reactors)
        r->stop();
    for (auto &t : reactor_threads)
        if (t.joinable())
            t.join();
    metrics_printer.join();
    LOG_INFO("server stopped");
    return 0;
}