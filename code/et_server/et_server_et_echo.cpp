// et_server_et_echo_diag.cpp
// 基于原 et_server_et_echo_fixed.cpp 的诊断版本：在 consume/steal/parse 等关键处加入校验与日志，便于定位 unexpect 4 的原因
// 编译：g++ -std=c++11 -O2 et_server_et_echo_diag.cpp -lpthread -o server

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

    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("et_log_diag.txt", true);
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

// -------------------------- Config --------------------------
static const size_t SMALL_BLOCK = 4096; // 小块
static const size_t LARGE_BLOCK = 4096*32; // 大块
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
        backing_.resize(total_blocks * block_size);
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
    uint8_t body_offset;
    uint32_t body_len;
};
#pragma pack(pop)

// -------------------------- RingBuffer（零拷贝入站，支持双层块） --------------------------
/*
常见 ring buffer 里“少 1 字节”的原因：
在很多“环形缓冲区（circular buffer）”的实现里，会故意浪费掉 1 个字节：
这样能区分 满（full） 和 空（empty） 两种状态。
因为如果允许写满，那么 head == tail 可能既代表“满”也可能代表“空”，就要额外一个标志位来区分。
所以一些实现干脆只允许最多写 cap-1 个字节，把最后一个字节当成哨兵。

但是！你这里的 RingBuffer 并不是传统的单缓冲区环形队列，而是：
一个由 BufferBlock 链表拼接起来的“分段环形”结构；
每个 BufferBlock 本身用 head_off_ / tail_off_ 来指示读写位置；
readable_bytes()、consume()、produce() 都是靠偏移量来算可读可写的；
没有依赖 head == tail 来判断状态。
所以你完全可以用满 cap 个字节，不需要浪费掉最后 1 个字节。
*/
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
        return iovec{tail_->data + tail_off_, tail_->cap - tail_off_/* - 1*/};
    }

    // 标记已写入 n 字节，根据需要扩展块
    void produce(size_t n) {
        tail_off_ += n;
        if (tail_off_ >= tail_->cap/* - 1*/) {
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
        // 诊断前后的可读字节数，便于断言
        size_t before = readable_bytes();
        size_t remain = n;
        while (remain > 0 && head_) {
            size_t avail = (head_ == tail_) ? (tail_off_ - head_off_) : (head_->cap - head_off_);
            if (avail > remain) {
                head_off_ += remain;
                remain = 0;
                break;
            }
            // avail <= remain 时，移除当前块
            remain -= avail;
            BufferBlock *old = head_;
            head_ = old->next;          // head 指向下一块
            pool_.release_ref(old);     // 回收已读完的块
            head_off_ = 0;              // 下一块的起始地址为 0
            if (!head_) {
                tail_ = nullptr;
                tail_off_ = 0;
                break;
            }
        }
        size_t after = readable_bytes();
        if (before >= n) {
            size_t expect = before - n;
            if (after != expect) {
                logger_->debug("consume mismatch: before={} consume={} after={} (head_off={}, tail_off={})",
                               before, n, after, head_off_, tail_off_);
                // 不中断运行，打印堆栈级别的诊断（如果需要可 assert）
                // assert(after == expect);
            }
        } else {
            // 试图 consume 超过可读数据，这本身就是错误
            logger_->debug("consume overrun: before={} requested={}", before, n);
            // assert(false);
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
        size_t before = readable_bytes();
        if (before < header_len + body_len)
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
            } else if (avail == skip) {
                // 恰好耗尽当前块，下一步从下一块开始拷贝
                skip = 0;
                cur = cur->next;
                off = 0;
                break;
            } else {
                skip -= avail;
                cur = cur->next;
                off = 0;
            }
        }

        // 拷贝 body_len 到 new_block->data
        size_t remain = body_len;
        size_t copied = 0;
        BufferBlock *copy_src = cur;
        size_t copy_off = off;
        while (remain > 0 && copy_src) {
            size_t avail = (copy_src == tail_) ? (tail_off_ - copy_off) : (copy_src->cap - copy_off);
            if (avail == 0) {
                // 不应该出现
                logger_->debug("steal_body_after: zero avail while copying (remain={}, copied={}, tail_off={}, head_off={})", remain, copied, tail_off_, head_off_);
                break;
            }
            size_t take = std::min(avail, remain);
            memcpy(new_block->data + copied, copy_src->data + copy_off, take);
            copied += take;
            remain -= take;
            copy_off += take;
            if (copy_off >= copy_src->cap) {
                copy_src = copy_src->next;
                copy_off = 0;
            }
        }

        if (copied != body_len) {
            // 拷贝不完整：释放 new_block 并返回 nullptr
            logger_->debug("steal_body_after copy incomplete: want={} got={} readable_before={}", body_len, copied, before);
            pool_.release_ref(new_block);
            return nullptr;
        }

        // 消耗 header_len + body_len
        consume(header_len + body_len);

        size_t after = readable_bytes();
        if (before - (header_len + body_len) != after) {
            logger_->debug("steal_body_after: readable mismatch before={} after={} consumed={}", before, after, header_len + body_len);
            // 进一步打印上下文
            // assert(before - (header_len + body_len) == after);
        }

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
        if (!tail_ || tail_off_ >= tail_->cap/* - 1*/) {
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
    size_t write_pos = 0; // 写入位置（下一个写入索引）
    size_t read_pos = 0;  // 已发送位置（下一个要发送的索引）
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
        write_pos = read_pos = 0;
        last_active_ms = 0;
    }

    // 写入待发送数据（从 worker 投递过来）
    bool out_push(const OutEntry &e, DualBufferPool &pool) {
        if (!out_buffer) {
            out_buffer = pool.acquire(LARGE_BLOCK);     //分配发送缓冲区
            if (!out_buffer){
                assert(0);
                return false;
            }
            write_pos = read_pos = 0;
        }
        // used = write_pos - read_pos
        size_t used = write_pos - read_pos;
        if (used + e.len > out_buffer->cap){
            assert(0);
            return false; // 缓冲区不足
        }
        memcpy(out_buffer->data + write_pos, e.blk->data + e.offset, e.len);
        write_pos += e.len;
        return true;
    }

    bool out_empty() const { return write_pos == read_pos; }

    iovec out_peek() {
        if (out_empty() || !out_buffer)
            return {nullptr, 0};
        return {out_buffer->data + read_pos, write_pos - read_pos};
    }

    void out_advance(size_t bytes_written, DualBufferPool &pool) {
        read_pos += bytes_written;
        if (read_pos >= write_pos) {
            // 全部发送完毕，释放缓冲
            read_pos = write_pos = 0;
            if (out_buffer) {
                pool.release_ref(out_buffer);
                out_buffer = nullptr;
            }
        }
        // 注意：不使用 memmove；保持 read_pos/write_pos 语义
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
    TaskQueue() : q_(1 << 16) {}

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
class Reactor {
public:
    Reactor(int cpu_id, int listen_fd, DualBufferPool &pool,
            ConnectionPool &cpool, WorkerPool &workers, uint64_t idle_ms,
            uint64_t active_ms)
        : cpu_id_(cpu_id), listen_fd_(listen_fd), pool_(pool), cpool_(cpool),
        workers_(workers), idle_ms_(idle_ms), active_ms_(active_ms), taskq_(),
        wheel_(100, 1024) {}

    void run() {
        pin_cpu(cpu_id_);
        epfd_ = epoll_create1(EPOLL_CLOEXEC);
        if (epfd_ < 0)
            die("epoll_create1");

        // 监听 fd 必须非阻塞且 EPOLLET
        epoll_event ev{}; 
        ev.events = EPOLLIN | EPOLLET;
        ev.data.fd = listen_fd_;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0)
            die("epoll_ctl listen");

        // timerfd（用于 tick），LT 即可
        int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        if (tfd < 0)
            die("timerfd_create");
        itimerspec its{};
        its.it_interval = {0, 100 * 1000 * 1000};
        its.it_value = its.it_interval;
        timerfd_settime(tfd, 0, &its, nullptr);
        epoll_event tev{};
        tev.events = EPOLLIN;
        tev.data.fd = tfd;
        epoll_ctl(epfd_, EPOLL_CTL_ADD, tfd, &tev);

        // metrics socket（loopback）
        /*int mfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
        if (mfd >= 0) {
            int yes = 1;
            setsockopt(mfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
            sockaddr_in ma{};
            ma.sin_family = AF_INET;
            ma.sin_port = htons(metrics_port_);
            ma.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
            if (bind(mfd, (sockaddr *)&ma, sizeof ma) == 0 && listen(mfd, 64) == 0) {
                epoll_event mev{};
                mev.events = EPOLLIN;
                mev.data.fd = mfd;
                if (epoll_ctl(epfd_, EPOLL_CTL_ADD, mfd, &mev) == 0)
                    metrics_fd_ = mfd;
                else
                    close(mfd);
            } else {
                close(mfd);
            }
        }*/

        std::vector<epoll_event> evs(4096);
        std::unordered_map<int, Connection *> conns;

        logger_->debug("run");
        while (!global_stop_.load()) {
            // 先处理 worker 发回的响应，减少尾延迟
            auto start_time = std::chrono::system_clock::now();

            process_incoming_tasks(conns);

            int n = epoll_wait(epfd_, evs.data(), (int)evs.size(), 100);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                die("epoll_wait");
            }
            uint64_t tnow = now_ms();

            for (int i = 0; i < n; ++i) {
                int fd = evs[i].data.fd;
                uint32_t events = evs[i].events;
                if (fd == listen_fd_) {
                    logger_->debug("accept_loop start");
                    accept_loop(conns);
                } else if (fd == tfd) {
                    uint64_t exp;
                    (void)read(tfd, &exp, sizeof(exp));
                    wheel_.tick(tnow, [&](int xfd) { on_timeout(xfd, conns); });
                } else if (fd == metrics_fd_) {
                    serve_metrics(fd);
                } else {
                    auto it = conns.find(fd);
                    if (it == conns.end())
                        continue;
                    auto *conn = it->second;
                    if (events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
                        close_conn(conn, conns);
                        g_metrics.in_ev.fetch_sub(1, std::memory_order_relaxed);
                        continue;
                    }
                    if (events & EPOLLOUT){
                        g_metrics.out_ev.fetch_sub(1, std::memory_order_relaxed);
                        on_writable(conn, conns); // [ET-CRITICAL] write 循环到 EAGAIN 或队列空
                    }
                    if (events & EPOLLIN){
                        g_metrics.in_ev.fetch_sub(1, std::memory_order_relaxed);
                        on_readable(conn, conns); // [ET-CRITICAL] read 循环到 EAGAIN
                    }
                }
            }

            auto end_time = std::chrono::system_clock::now();
            uint64_t elapse = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        }

        logger_->debug("clean");
        for (auto &p : conns)
            close_conn(p.second, conns);
        if (metrics_fd_ >= 0)
            close(metrics_fd_);
        close(epfd_);

        logger_->debug("exit");
    }

    void stop() { global_stop_.store(true); }
    void set_metrics_port(int p) { metrics_port_ = p; }
    bool enqueue_response(const ResponseTask &t) { return taskq_.enqueue(t); }

private:
    int cpu_id_;
    int epfd_ = -1;
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

    static void pin_cpu(int cpu) {
#ifdef __linux__
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        sched_setaffinity(0, sizeof(set), &set);
#endif
    }

    // [ET-CRITICAL] 批量 accept：循环最多 MAX_ACCEPT_BATCH 次，并在 EAGAIN 时退出
    void accept_loop(std::unordered_map<int, Connection *> &conns) {
        for (int i = 0; i < MAX_ACCEPT_BATCH; ++i) {
            sockaddr_in in{};
            socklen_t inlen = sizeof(in);
            int cfd = accept4(listen_fd_, (sockaddr *)&in, &inlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (cfd < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK){
                    break;
                }
                if (errno == EINTR)
                    continue;
                else
                    break;
            }
            set_tcp_options(cfd);
            Connection *conn = nullptr;
            if (!cpool_.admit(cfd, &conn)) {
                g_metrics.closed.fetch_add(1, std::memory_order_relaxed);
                ::close(cfd);
                logger_->debug("unexpect 7");
                continue;
            }

            if (!add_event(epfd_, cfd, EPOLLIN | EPOLLRDHUP | EPOLLET | EPOLLONESHOT)) {
                assert(0);
                ::close(cfd);
                cpool_.release_ref(conn);
                continue;
            }
            g_metrics.in_ev.fetch_add(1, std::memory_order_relaxed);

            conns[cfd] = conn;
            g_metrics.accepted.fetch_add(1, std::memory_order_relaxed);
            wheel_.add(cfd, now_ms() + active_ms_);
        }
    }

    // [ET-CRITICAL] read 必须拉空直到 EAGAIN
    void on_readable(Connection *conn, std::unordered_map<int, Connection*> &conns) {
        //logger_->debug("on_readable start");

        bool bNeedRead = false;
        for (;;) {
            iovec w = conn->rx->writable_region(SMALL_BLOCK); // 先用小块读头
            if (w.iov_len == 0){
                assert(0);
                break;
            }
            ssize_t n = ::read(conn->fd, w.iov_base, w.iov_len);
            if (n > 0) {
                conn->rx->produce((size_t)n);
                g_metrics.rx_bytes += (uint64_t)n;
                conn->last_active_ms.store(now_ms(), std::memory_order_relaxed);
                continue;
            }
            if (n == 0) {   //客户端关闭了连接
                close_conn(conn, conns);
                logger_->debug("unexpect 2");
                return;
            }
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK){
                    bNeedRead = true;
                    break;
                }
                if (errno == EINTR)
                    continue;
                close_conn(conn, conns);
                logger_->debug("unexpect 3");
                return;
            }
        }

        const size_t H = 1 + 4; // magic + body_len
        while (true) {
            if (conn->rx->readable_bytes() < H){
                bNeedRead = true;
                break;
            }
            uint8_t hdrbuf[5];
            size_t got = conn->rx->peek_bytes(hdrbuf, H);
            if (got < H){
                assert(0);
                break;
            }
            PacketHeader hdr;
            hdr.body_offset = hdrbuf[0];
            uint32_t bl;
            memcpy(&bl, hdrbuf + 1, 4);
            hdr.body_len = bl;
            const uint32_t MAX_BODY = 16 * 1024 * 1024;
            if (hdr.body_len > MAX_BODY) {
                g_metrics.parse_errors.fetch_add(1, std::memory_order_relaxed);
                // 打印原始 header 和当前可读字节，用于诊断是否错位
                logger_->debug("unexpect 4: raw header bytes = {:02x} {:02x} {:02x} {:02x} {:02x}, parsed_body_len={}, readable={}",
                               hdrbuf[0], hdrbuf[1], hdrbuf[2], hdrbuf[3], hdrbuf[4], hdr.body_len, conn->rx->readable_bytes());
                close_conn(conn, conns);
                return;
            }
            if (conn->rx->readable_bytes() < H + hdr.body_len){
                bNeedRead = true;
                break;
            }

            // consume header
            /*size_t before_consume = conn->rx->readable_bytes();
            conn->rx->consume(H);
            size_t after_consume = conn->rx->readable_bytes();
            if (before_consume - H != after_consume) {
                logger_->debug("consume header mismatch: before={} after={} H={}", before_consume, after_consume, H);
            }*/

            // 读包体阶段提示使用大块以提升吞吐，并不一定能实现使用大块读
            (void)conn->rx->writable_region(std::min<size_t>(
                LARGE_BLOCK, std::max<size_t>(SMALL_BLOCK, hdr.body_len)));

            // steal body 拷贝到新块
            BufferBlock *stolen = conn->rx->steal_body_after(0, H+hdr.body_len);
            if (!stolen) {
                g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
                logger_->debug("steal failed for fd={} want_len={} readable={}", conn->fd, H+hdr.body_len, conn->rx->readable_bytes());
                break;
            }
            g_metrics.rx_pkts.fetch_add(1, std::memory_order_relaxed);

            // 把工作投递给 worker: long time work
            /*auto job = [fd = conn->fd, stolen, body_len = hdr.body_len, this]() {
                business_worker_echo(fd, stolen, body_len, taskq_);
            };
            workers_.submit(job);*/

            //short time work
            //std::string msg = "receive ";
            //msg.append((char*)stolen->data, hdr.body_len);
            //logger_->debug(msg);
            business_worker_echo(conn->fd, stolen, hdr.body_len, taskq_);
        }

        uint32_t events = EPOLLRDHUP | EPOLLET | EPOLLONESHOT;
        events |= EPOLLIN;
        g_metrics.in_ev.fetch_add(1, std::memory_order_relaxed);

        if(!mod_event(epfd_, conn->fd, events))
            assert(0);
    }

    // [ET-CRITICAL] write 必须写到 EAGAIN 或队列空
    void on_writable(Connection *conn, std::unordered_map<int, Connection*> &conns) {
        //logger_->debug("on_writable start");

        bool bNeedWrite = false;
        while (!conn->out_empty()) {
            iovec iov = conn->out_peek();
            if (iov.iov_len == 0){
                assert(0);
                break;
            }

            ssize_t n = ::write(conn->fd, iov.iov_base, iov.iov_len);
            if (n > 0) {
                g_metrics.tx_bytes += (uint64_t)n;
                g_metrics.tx_pkts.fetch_add(1, std::memory_order_relaxed);
                conn->out_advance((size_t)n, pool_);
                continue;
            }
            if (n < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK){
                    bNeedWrite = true;
                    assert(0);
                    break;
                }

                close_conn(conn, conns);
                logger_->debug("unexpect 5");
                return;
            }
        }
    }

    void on_timeout(int fd, std::unordered_map<int, Connection *> &conns) {
        auto it = conns.find(fd);
        if (it == conns.end())
            return;
        if (now_ms() - it->second->last_active_ms > idle_ms_) {     //判定是否超时
            g_metrics.timeouts.fetch_add(1, std::memory_order_relaxed);
            close_conn(it->second, conns);
            g_metrics.in_ev.fetch_sub(1, std::memory_order_relaxed);
            logger_->debug("unexpect 6");
        } else {
            it->second->last_active_ms.store(now_ms(), std::memory_order_relaxed);
            wheel_.add(fd, now_ms() + active_ms_);
        }
    }

    void process_incoming_tasks(std::unordered_map<int, Connection *> &conns) {
        ResponseTask t;
        bool ret = false;
        while (ret = taskq_.dequeue(t)) {
            auto it = conns.find(t.fd);
            if (it == conns.end()) {
                pool_.release_ref(t.entry.blk);
                logger_->debug("unexpected 1");
                continue;
            }
            Connection *conn = it->second;
            if (!conn->out_push(t.entry, pool_)) {
                g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
                pool_.release_ref(t.entry.blk);
                logger_->debug("unexpected 2");
                continue;
            }

            // 发送数据（尽可能写到 EAGAIN）
            on_writable(conn, conns);

            if (!conn->out_empty()){
                uint32_t events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET | EPOLLONESHOT;
                g_metrics.out_ev.fetch_add(1, std::memory_order_relaxed);

                if(!mod_event(epfd_, conn->fd, events))
                    assert(0);
            }

            // 业务的 stolen block 在上面已经拷贝到 out_buffer，可以释放
            pool_.release_ref(t.entry.blk);
        }
    }

    void serve_metrics(int mfd) {
        for (;;) {
            sockaddr_in cli;
            socklen_t len = sizeof cli;
            int c = accept4(mfd, (sockaddr *)&cli, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
            if (c < 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                    break;
                else
                    break;
            }
            char buf[1024];
            int n = (int)recv(c, buf, sizeof(buf) - 1, 0);
            if (n <= 0) {
                close(c);
                continue;
            }
            buf[n] = 0;
            if (strncmp(buf, "GET /metrics", 12) == 0) {
                std::string body = metrics_text();
                char hdr[256];
                int hn = snprintf(hdr, sizeof hdr,
                                  "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: "
                                  "text/plain; version=0.0.4\r\nConnection: close\r\n\r\n",
                                  body.size());
                send(c, hdr, hn, 0);
                send(c, body.data(), body.size(), 0);
            } else {
                const char *r = "HTTP/1.1 404\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                send(c, r, strlen(r), 0);
            }
            close(c);
        }
    }

    void close_conn(Connection *conn, std::unordered_map<int, Connection *> &conns) {
        if (!conn)
            return;
        g_metrics.closed.fetch_add(1, std::memory_order_relaxed);

        if (conn->out_buffer) {
            pool_.release_ref(conn->out_buffer);
            conn->out_buffer = nullptr;
        }

        if(epoll_ctl(epfd_, EPOLL_CTL_DEL, conn->fd, nullptr))
        {
            char* str = strerror(errno);
            fprintf(stderr, str);
            assert(0);
        }
        conns.erase(conn->fd);
        cpool_.release_ref(conn);
    }

    void throw_close(Connection *conn) { close_conn(conn, *(std::unordered_map<int, Connection *> *)nullptr); }
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

    size_t max_conn = 1000;
    int ncpu = get_nprocs();
    ncpu = 2;
    size_t small_blocks = (size_t)ncpu * max_conn * 4; // 可按内存和连接数调节
    size_t large_blocks = (size_t)ncpu * max_conn / 4;

    LOG_INFO("ET-opt server starting on port %u with %d CPUs, small_blocks=%zu, "
             "large_blocks=%zu",
             port, ncpu, small_blocks, large_blocks);

    DualBufferPool pool(small_blocks, large_blocks);
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
