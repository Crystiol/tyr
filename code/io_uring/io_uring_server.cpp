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
#include <condition_variable>

#include "liburing.h"
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"

using namespace std::chrono;
using namespace std::chrono_literals;

// -------------------------- 基础工具 & 日志 --------------------------
std::shared_ptr<spdlog::logger> logger_;
void init_logger() {
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console_sink->set_level(spdlog::level::debug);
    console_sink->set_pattern("[et_server] [%^%l%$] %v");

    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("et_log.txt", true);
    file_sink->set_level(spdlog::level::trace);

    logger_ = std::make_shared<spdlog::logger>("multi_sink", spdlog::sinks_init_list{console_sink, file_sink});
    logger_->set_level(spdlog::level::debug);
    logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [tid %t] [%^%l%$] %v");
}

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

bool add_event(int epfd, int fd, uint32_t events) {
    struct epoll_event ev;
    ev.events = events;       // 初始注册的事件
    ev.data.fd = fd;
    if (::epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev) == -1) {
        assert(0);
        return false;
    }
    return true;
}

bool mod_event(int epfd, int fd, uint32_t events) {
    struct epoll_event ev;
    ev.events = events;
    ev.data.fd = fd;
    if (::epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev) == -1) {
        assert(0);
        return false;
    }
    return true;
}

void del_event(int epfd, int fd) {
    if (::epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr) == -1) {
        assert(0);
    }
}


void print_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    // 转换为本地时间
    struct tm tm_info;
    localtime_r(&tv.tv_sec, &tm_info);

    fprintf(stderr,"[Tid=%-6d %04d-%02d-%02d %02d:%02d:%02d.%03ld] ", gettid(),
            tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday, tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, tv.tv_usec / 1000);
}

class Elapse
{
public:
    Elapse(const std::string& func_name) : func_(func_name){
        start_time_ = std::chrono::system_clock::now();
    }

    ~Elapse()
    {
        end_time_= std::chrono::system_clock::now();
        uint64_t elapse =  std::chrono::duration_cast<std::chrono::milliseconds>(end_time_ - start_time_).count();

        print_time();
        fprintf(stderr,"%s %lu\n", func_.c_str(), elapse);
    }

private:
    std::chrono::system_clock::time_point start_time_;
    std::chrono::system_clock::time_point end_time_;
    std::string func_;
};

/*
 * active_ms < idle_ms → 检查更频繁，超时触发更接近真实 idle_ms。
 * active_ms > idle_ms → 检查周期太长，会延迟发现超时，延迟时间最多等于 active_ms。
*/

// -------------------------- Config --------------------------
static const size_t SMALL_BLOCK = 512; // 小块：协议头或小包512
static const size_t LARGE_BLOCK = 8096; // 大块：主体或大包4096
static const int MAX_ACCEPT_BATCH = 128; // 每次 accept 最多尝试次数
static const size_t OUT_BUFFER_CAP = LARGE_BLOCK; // 出站缓冲区容量（单一 BufferBlock）
static const uint64_t DEFAULT_IDLE_MS = 30 * 1000;
static const uint64_t DEFAULT_ACTIVE_MS = 1 * 60 * 1000;

// -------------------------- Metrics --------------------------
struct Metrics {
    std::atomic<uint64_t> accepted{0}, closed{0}, rx_bytes{0}, tx_bytes{0},
        rx_pkts{0}, tx_pkts{0}, drops{0}, parse_errors{0}, timeouts{0}, s_pool{0}, l_pool{0}, r_pool{0}, c_pool{0}, in_ev{0}, out_ev{0};
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
// #pragma pack(push, 1)
// struct PacketHeader {
//     uint8_t magic;
//     uint32_t body_len;
//     uint8_t endian;
//     uint32_t hdr_crc;
// };
//#pragma pack(pop)
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t body_offset;
    uint32_t body_len;
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
        }
        for (size_t i = 0; i + 1 < total; ++i) {
            storage_[i]->next = storage_[i + 1].get();
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
    }

    bool out_push(const OutEntry &e, DualBufferPool &pool) {
        if (!out_buffer) {
            out_buffer = pool.acquire(LARGE_BLOCK);     //分配发送缓冲区
            if (!out_buffer){
                assert(0);
                return false;
            }
        }
        if (out_offset + e.len > out_buffer->cap){
            assert(0);
            return false; // 缓冲区不足
        }
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
        //return {out_buffer->data, out_len};
        return {out_buffer->data, 8};			//测试收发，实际上客户端可能一次性读取返回数据，如果采用了类似et的方式
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
        }
        for (size_t i = 0; i + 1 < max_conn; ++i) {
            storage_[i]->next = storage_[i + 1].get();
        }
        freelist_.store(storage_[0].get(), std::memory_order_release);
        g_metrics.c_pool.store(max_conn);
    }

    bool admit(int fd, Connection **out_conn) {
        if (active_.load(std::memory_order_relaxed) >= max_active_){
            logger_->debug("admit 1");
            return false;
        }

        Connection *conn = acquire();
        if (!conn){
            logger_->debug("admit 2");
            return false;
        }

        RingBuffer *rb = rp_.acquire();
        if (!rb) {
            logger_->debug("admit 3");
            release_ref(conn);
            return false;
        }

        conn->fd = fd;
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
        if (cur >= max_active_){
            logger_->debug("acquire 1");
            return nullptr;
        }

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
        logger_->debug("acquire 2: {} {}", active_.load(), g_metrics.c_pool.load());
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
        int fd;
        uint64_t expire_ms;
    };

    explicit TimingWheel(uint64_t tick_ms, size_t slots)
        : tick_ms_(tick_ms), slots_(normalize_pow2(slots)), slot_mask_(slots_ - 1),
          initialized_(false), last_slot_index_(0) {
        slots_q_.reserve(slots_);
        for (size_t i = 0; i < slots_; ++i){
            slots_q_.emplace_back(std::make_unique<MPMCRing<Entry>>(1 << 14));
        }
    }

    /*
     * 加入时间轮意味着active_ms后进行检查，如果在检查时发现距检查点已经过了idle_ms的时间，
     * 则判定为超时，如果未到idle_ms的时间，那么就在下一个active_ms时再检查，实际上超时的
	 * 时间是active_ms+idle_ms
    */
    inline void add(int fd, uint64_t expire_ms) {   //记录检查的时间，放到对应的槽中
        Entry e{fd, expire_ms};
        size_t idx = slot_index(expire_ms);
        for (int i = 0; i < 64; ++i) {
            if (slots_q_[idx]->enqueue(e))
                return;
            std::this_thread::yield();
        }
        g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
    }

    template<typename F>
    void tick(uint64_t now_ms, F on_timeout) {		//开始检查
        //fprintf(stderr,"tick1\n");
        if (!initialized_.load(std::memory_order_acquire)) {
            size_t idx = (now_ms / tick_ms_) & slot_mask_;
            last_slot_index_.store(idx, std::memory_order_release);
            initialized_.store(true, std::memory_order_release);
            return;
        }
        //fprintf(stderr,"tick2\n");
        size_t target = (now_ms / tick_ms_) & slot_mask_;
        size_t cur = last_slot_index_.load(std::memory_order_relaxed);
        //fprintf(stderr,"tick2.1\n");
        while (cur != target) {
            drain(cur, now_ms, on_timeout);
            cur = (cur + 1) & slot_mask_;
        }
        //fprintf(stderr,"tick3\n");
        last_slot_index_.store(cur, std::memory_order_relaxed);
        drain_light(cur, now_ms, on_timeout);
        //fprintf(stderr,"tick4\n");
    }

private:
    /*template<typename F>
    void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        while (slots_q_[idx]->dequeue(e)) {
            if (e.expire_ms <= now_ms)
                on_timeout(e.fd);
            else
                add(e.fd, e.expire_ms);
        }
    }*/

    // template<typename F>
    // void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
    //     Entry e;
    //     while (slots_q_[idx]->dequeue(e)) {
    //         if (e.expire_ms <= now_ms) {
    //             on_timeout(e.fd);
    //         } else {
    //             // 如果条目计算出来的槽正好是当前正在 drain 的槽，
    //             // 那么把 expire_ms 向后推进一个 tick，避免被立即再次处理。
    //             size_t target_idx = slot_index(e.expire_ms);
    //             if (target_idx == idx) {
    //                 e.expire_ms += tick_ms_;
    //             }
    //             add(e.fd, e.expire_ms);
    //         }
    //     }
    // }

    template<typename F>
    void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        while (slots_q_[idx]->dequeue(e)) {
            if (e.expire_ms <= now_ms) {
                // 已到期 → 直接触发
                on_timeout(e.fd);
            } else {
                // 还没到期 → 重新放入它对应的槽
                size_t target_idx = slot_index(e.expire_ms);
                slots_q_[target_idx]->enqueue(e);
            }
        }
    }

    template<typename F>
    void drain_light(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        for (int k = 0; k < 64; ++k) {
            if (!slots_q_[idx]->dequeue(e))
                break;
            if (e.expire_ms <= now_ms)
                on_timeout(e.fd);
            else
                add(e.fd, e.expire_ms);
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
    //logger_->debug("business_worker_echo");
    ResponseTask t;
    t.fd = fd;
    t.entry = {stolen_block, 0, body_len};
    while (!reactor_queue.enqueue(t)){
        std::this_thread::yield();
    }
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
        while (!queue_push(j)){
            std::this_thread::yield();
        }
    }

private:
    bool queue_push(const Job &j) { return queue_.push(j); }

    void loop() {
        Job j;
        while (!stop_.load()) {
            if (queue_pop(j)){
                j();
            }
            else{
                std::this_thread::yield();
            }
        }
    }

    bool queue_pop(Job &j) { return queue_.pop(j); }
    std::vector<std::thread> threads_;
    std::atomic<bool> stop_;
    WorkQueue<Job> queue_;
};

// -------------------------- Reactor（ET 核心） --------------------------
// 将下面这个类替换掉原文件中的 Reactor 类实现
class Reactor {
public:
    Reactor(int cpu_id, int listen_fd, DualBufferPool &pool,
            ConnectionPool &cpool, WorkerPool &workers, uint64_t idle_ms,
            uint64_t active_ms)
        : cpu_id_(cpu_id), listen_fd_(listen_fd), pool_(pool), cpool_(cpool),
          workers_(workers), idle_ms_(idle_ms), active_ms_(active_ms), taskq_(),
          wheel_(100, 1024) {
        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        io_uring_queue_init_params(128, &ring_, &params); // will be initialized in run()
    }

    ~Reactor() {
        // ensure ring cleaned if constructed
    }

    void run() {
        pin_cpu(cpu_id_);

        // init io_uring
        struct io_uring_params params;
        memset(&params, 0, sizeof(params));
        if (io_uring_queue_init_params(4096, &ring_, &params) < 0) {
            die("io_uring_queue_init_params");
        }

        // create timerfd for timingwheel ticks (we still use timerfd)
        tfd_ = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd_ < 0) die("timerfd_create");
        itimerspec its{};
        its.it_interval = {0, 100 * 1000 * 1000};
        its.it_value = its.it_interval;
        timerfd_settime(tfd_, 0, &its, nullptr);

        // metrics socket (loopback) - try to bind/listen and we will accept it via io_uring
        metrics_fd_ = -1;
        int mfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (mfd >= 0) {
            int yes = 1;
            setsockopt(mfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
            sockaddr_in ma{};
            ma.sin_family = AF_INET;
            ma.sin_port = htons(metrics_port_);
            ma.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            if (bind(mfd, (sockaddr *)&ma, sizeof ma) == 0 && listen(mfd, 64) == 0) {
                metrics_fd_ = mfd;
            } else {
                close(mfd);
            }
        }

        // structures
        std::unordered_map<int, Connection *> conns;
        logger_->debug("run using io_uring");

        // submit initial accept on listen_fd
        submit_accept();

        // submit initial read on timerfd so we get ticks via completions
        submit_read_fd(tfd_, nullptr, sizeof(uint64_t), EventTag::TIMER);

        // if we have metrics listener, submit accept for it
        if (metrics_fd_ >= 0) {
            submit_accept_fd(metrics_fd_, EventTag::METRICS_LISTEN, nullptr);
        }

        // main loop
        while (!global_stop_.load()) {
            // 1) 先处理 worker 发回的响应（减少尾延迟）
            process_incoming_tasks_and_submit_writes(conns);

            // 2) 调用 io_uring_wait_cqe 或 io_uring_submit_and_wait(1) 以获得至少一个完成
            struct io_uring_cqe *cqe = nullptr;
            int ret = io_uring_submit_and_wait(&ring_, 1);
            if (ret < 0) {
                if (errno == EINTR) continue;
                die("io_uring_submit_and_wait");
            }

            unsigned head;
            unsigned count = 0;
            io_uring_for_each_cqe(&ring_, head, cqe) {
                ++count;
                // handle completion
                handle_cqe(cqe, conns);
            }
            if (count > 0) {
                io_uring_cq_advance(&ring_, count);
            }
        }

        // cleanup
        logger_->debug("clean");
        for (auto &p : conns) {
            close_conn(p.second, conns);
        }
        if (metrics_fd_ >= 0) close(metrics_fd_);
        close(tfd_);
        io_uring_queue_exit(&ring_);
        logger_->debug("exit");
    }

    void stop() { global_stop_.store(true); }
    void set_metrics_port(int p) { metrics_port_ = p; }
    bool enqueue_response(const ResponseTask &t) { return taskq_.enqueue(t); }

private:
    // small enum to tag operations in user_data
    enum class EventTag : uint64_t {
        ACCEPT = 1,
        READ = 2,
        WRITE = 3,
        TIMER = 4,
        METRICS_ACCEPT = 5,
        METRICS_READ = 6,
        METRICS_LISTEN = 7
    };

    struct IoUser {
        EventTag tag;
        int fd; // target fd (for some ops)
        Connection *conn; // optional, for READ/WRITE
    };

    // submit helper: allocate sqe and attach IoUser via user_data pointer
    void submit_accept() {
        submit_accept_fd(listen_fd_, EventTag::ACCEPT, nullptr);
    }

    void submit_accept_fd(int fd, EventTag tag, void* userptr) {
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if (!sqe) {
            // ring full -> submit and retry
            io_uring_submit(&ring_);
            sqe = io_uring_get_sqe(&ring_);
            if (!sqe) die("get_sqe accept");
        }
        IoUser *u = new IoUser{tag, fd, nullptr};
        io_uring_prep_accept(sqe, fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        io_uring_sqe_set_data(sqe, u);
        io_uring_submit(&ring_);
    }

    void submit_read_fd(int fd, Connection *conn, size_t len, EventTag tag) {
        // prepare a read into a small stack buffer or into RingBuffer writable region
        // For timerfd and metrics we read into small stack buffer; for connection->rx use iovec from RingBuffer
        if (tag == EventTag::TIMER || tag == EventTag::METRICS_READ) {
            uint64_t *buf = new uint64_t;
            struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
            if (!sqe) { io_uring_submit(&ring_); sqe = io_uring_get_sqe(&ring_); if (!sqe) die("get_sqe readfd"); }
            IoUser *u = new IoUser{tag, fd, nullptr};
            io_uring_prep_read(sqe, fd, buf, sizeof(uint64_t), 0);
            io_uring_sqe_set_data(sqe, u);
            io_uring_submit(&ring_);
            return;
        }

        // connection io read (use readv into ringbuffer region)
        iovec w = conn->rx->writable_region(SMALL_BLOCK);
        if (w.iov_len == 0) return; // shouldn't happen
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if (!sqe) { io_uring_submit(&ring_); sqe = io_uring_get_sqe(&ring_); if (!sqe) die("get_sqe readv"); }
        IoUser *u = new IoUser{EventTag::READ, conn->fd, conn};
        struct iovec *iov = new struct iovec;
        *iov = w;
        io_uring_prep_readv(sqe, conn->fd, iov, 1, 0);
        io_uring_sqe_set_data(sqe, u);
        // store iov pointer in u->conn temporarily via conn pointer? we'll delete iov after completion
        io_uring_submit(&ring_);
    }

    void submit_write(Connection *conn) {
        if (conn->out_empty()) return;
        iovec iov = conn->out_peek();
        if (iov.iov_len == 0) return;
        struct io_uring_sqe *sqe = io_uring_get_sqe(&ring_);
        if (!sqe) { io_uring_submit(&ring_); sqe = io_uring_get_sqe(&ring_); if (!sqe) die("get_sqe write"); }
        IoUser *u = new IoUser{EventTag::WRITE, conn->fd, conn};
        // allocate iovec copy on heap so it survives until completion
        struct iovec *piov = new struct iovec;
        *piov = iov;
        io_uring_prep_writev(sqe, conn->fd, piov, 1, 0);
        io_uring_sqe_set_data(sqe, u);
        io_uring_submit(&ring_);
    }

    void handle_cqe(struct io_uring_cqe *cqe, std::unordered_map<int, Connection *> &conns) {
        IoUser *u = static_cast<IoUser *>(io_uring_cqe_get_data(cqe));
        int res = cqe->res;

        if (!u) {
            // unexpected
            return;
        }

        switch (u->tag) {
            case EventTag::ACCEPT:
            case EventTag::METRICS_LISTEN:
            case EventTag::METRICS_ACCEPT: {
                // accept returned new fd (or error)
                int cfd = res;
                if (cfd >= 0) {
                    if (u->tag == EventTag::METRICS_LISTEN) {
                        // metrics connection accepted; handle in place
                        handle_metrics_conn(cfd);
                        close(cfd);
                    } else {
                        // normal accepted connection
                        set_tcp_options(cfd);
                        Connection *conn = nullptr;
                        if (!cpool_.admit(cfd, &conn)) {
                            g_metrics.closed.fetch_add(1, std::memory_order_relaxed);
                            ::close(cfd);
                        } else {
                            conns[cfd] = conn;
                            g_metrics.accepted.fetch_add(1, std::memory_order_relaxed);
                            wheel_.add(cfd, now_ms() + active_ms_);
                            // immediately submit a read for this conn
                            submit_read_fd(cfd, conn, SMALL_BLOCK, EventTag::READ);
                        }
                    }
                } else {
                    // accept error - if transient, ignore. We will resubmit an accept anyway.
                }
                // resubmit accept on listen_fd (always keep accepting)
                submit_accept();
                break;
            }
            case EventTag::TIMER: {
                // timerfd read completed; resubmit read to keep getting ticks
                if (res >= 0) {
                    uint64_t tnow = now_ms();
                    wheel_.tick(tnow, [&](int xfd) { on_timeout(xfd, conns); });
                }
                // resubmit timer read
                submit_read_fd(tfd_, nullptr, sizeof(uint64_t), EventTag::TIMER);
                break;
            }
            case EventTag::READ: {
                Connection *conn = u->conn;
                if (!conn) { delete u; break; }
                if (res > 0) {
                    // consumed res bytes into conn->rx: we must adjust ringbuffer's produce
                    conn->rx->produce((size_t)res);
                    g_metrics.rx_bytes += (uint64_t)res;
                    conn->last_active_ms.store(now_ms(), std::memory_order_relaxed);

                    // now process available complete packets (reuse on_readable logic)
                    // copy-paste of on_readable's packet parsing loop but limited to current thread
                    const size_t H = 1 + 4;
                    while (true) {
                        if (conn->rx->readable_bytes() < H) break;
                        uint8_t hdrbuf[5];
                        size_t got = conn->rx->peek_bytes(hdrbuf, H);
                        if (got < H) break;
                        PacketHeader hdr;
                        hdr.body_offset = hdrbuf[0];
                        uint32_t bl;
                        memcpy(&bl, hdrbuf + 1, 4);
                        hdr.body_len = bl;
                        const uint32_t MAX_BODY = 16 * 1024 * 1024;
                        if (hdr.body_len > MAX_BODY) {
                            g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
                            close_conn(conn, conns);
                            break;
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

                        business_worker_echo(conn->fd, stolen, hdr.body_len, taskq_);
                    }
                } else if (res == 0) {
                    // peer closed
                    close_conn(conn, conns);
                } else {
                    if (res == -EAGAIN || res == -EWOULDBLOCK) {
                        // nothing to read now
                    } else {
                        close_conn(conn, conns);
                    }
                }

                // resubmit a read for the connection if still present
                if (conns.find(u->fd) != conns.end()) {
                    // safe to resubmit read
                    Connection *maybe = conns[u->fd];
                    if (maybe) submit_read_fd(u->fd, maybe, SMALL_BLOCK, EventTag::READ);
                }
                break;
            }
            case EventTag::WRITE: {
                Connection *conn = u->conn;
                if (!conn) { delete u; break; }
                if (res >= 0) {
                    g_metrics.tx_bytes += (uint64_t)res;
                    g_metrics.tx_pkts.fetch_add(1, std::memory_order_relaxed);
                    conn->out_advance((size_t)res, pool_);
                    // if still data, resubmit write
                    if (!conn->out_empty()) {
                        submit_write(conn);
                    }
                } else {
                    if (res == -EAGAIN || res == -EWOULDBLOCK) {
                        // will try later (no epoll needed)
                    } else {
                        close_conn(conn, conns);
                    }
                }
                break;
            }
            case EventTag::METRICS_READ: {
                // metrics read: similar to old serve_metrics but simplified
                // we allocated a uint64_t pointer for read; find the fd from u->fd and then accept served connection earlier
                // ignore for now
                break;
            }
            default:
                break;
        }

        delete u;
    }

    void process_incoming_tasks_and_submit_writes(std::unordered_map<int, Connection *> &conns) {
        ResponseTask t;
        bool ret = false;
        while (ret = taskq_.dequeue(t)) {
            auto it = conns.find(t.fd);
            if (it == conns.end()) {
                pool_.release_ref(t.entry.blk);
                continue;
            }
            Connection *conn = it->second;
            if (!conn->out_push(t.entry, pool_)) {
                g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
                pool_.release_ref(t.entry.blk);
                continue;
            }
            pool_.release_ref(t.entry.blk);
            // submit write via io_uring
            submit_write(conn);
        }
    }

    void handle_metrics_conn(int cfd) {
        // simple blocking-ish read (metrics connections are rare). Do minimal handling:
        char buf[1024];
        int n = (int)recv(cfd, buf, sizeof(buf) - 1, 0);
        if (n <= 0) return;
        buf[n] = 0;
        if (strncmp(buf, "GET /metrics", 12) == 0) {
            std::string body = metrics_text();
            char hdr[256];
            int hn = snprintf(hdr, sizeof hdr,
                              "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: "
                              "text/plain; version=0.0.4\r\nConnection: close\r\n\r\n",
                              body.size());
            send(cfd, hdr, hn, 0);
            send(cfd, body.data(), body.size(), 0);
        } else {
            const char *r = "HTTP/1.1 404\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
            send(cfd, r, strlen(r), 0);
        }
    }

    void close_conn(Connection *conn, std::unordered_map<int, Connection *> &conns) {
        if (!conn) return;
        g_metrics.closed.fetch_add(1, std::memory_order_relaxed);

        if (conn->out_buffer) {
            pool_.release_ref(conn->out_buffer);
            conn->out_buffer = nullptr;
        }
        int fd = conn->fd;
        // remove from map if present
        auto it = conns.find(fd);
        if (it != conns.end()) conns.erase(it);
        cpool_.release_ref(conn);
        ::close(fd);
    }

    void on_timeout(int fd, std::unordered_map<int, Connection *> &conns) {
        auto it = conns.find(fd);
        if (it == conns.end())
            return;
        if (now_ms() - it->second->last_active_ms > idle_ms_) {
            g_metrics.timeouts.fetch_add(1, std::memory_order_relaxed);
            close_conn(it->second, conns);
        } else {
            it->second->last_active_ms.store(now_ms(), std::memory_order_relaxed);
            wheel_.add(fd, now_ms() + active_ms_);
        }
    }

    // helpers
    static void pin_cpu(int cpu) {
#ifdef __linux__
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        sched_setaffinity(0, sizeof(set), &set);
#endif
    }

    // members
    int cpu_id_;
    int listen_fd_;
    DualBufferPool &pool_;
    ConnectionPool &cpool_;
    WorkerPool &workers_;
    uint64_t idle_ms_, active_ms_;
    TaskQueue taskq_;
    TimingWheel wheel_;
    std::atomic<bool> global_stop_{false};
    int metrics_fd_ = -1;
    int metrics_port_ = 9100;

    // io_uring
    struct io_uring ring_;
    int tfd_ = -1;
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

    init_logger();

    int ncpu = get_nprocs();
    ncpu = 2;
    size_t small_blocks = (size_t)ncpu * 32 * 1000; // 可按内存和连接数调节
    size_t large_blocks = (size_t)ncpu * 16 * 1000;

    LOG_INFO("ET-opt server starting on port %u with %d CPUs, small_blocks=%zu, "
             "large_blocks=%zu",
             port, ncpu, small_blocks, large_blocks);

    DualBufferPool pool(small_blocks, large_blocks);
    size_t max_conn = 1000;
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
    if (listen(listen_fd, 65535) < 0)
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
            std::this_thread::sleep_for(std::chrono::seconds(4));
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

            logger_->debug(
                    "acc={} cls={} rx={} tx={} rx/s={:.2f}B "
                    "tx/s={:.2f}B pkts(rx={} tx={}) drop={} err={} to={} sp={} lp={} rp={} cp={} in={} out={}",
                    g_metrics.accepted.load(),
                    g_metrics.closed.load(),
                    rx, tx, rxrate, txrate,
                    g_metrics.rx_pkts.load(),
                    g_metrics.tx_pkts.load(),
                    g_metrics.drops.load(),
                    g_metrics.parse_errors.load(),
                    g_metrics.timeouts.load(),
                    g_metrics.s_pool.load(),
                    g_metrics.l_pool.load(),
                    g_metrics.r_pool.load(),
                    g_metrics.c_pool.load(),
                    g_metrics.in_ev.load(),
                    g_metrics.out_ev.load());
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
