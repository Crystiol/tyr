// epoll_server_prod_zero_copy_et_opt.cpp
// 单文件高性能零拷贝 Epoll 服务器（全 ET / 批量 accept / 双层 BufferPool）
// - 完整实现：BufferPool / RingBuffer / Connection / Reactor / WorkerPool /
// TimingWheel / metrics
// - 关键位置已标注 [ET-CRITICAL]
// 编译： g++ -std=c++11 -O2 epoll_server_prod_zero_copy_et_opt.cpp -lpthread -o
// server

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

using namespace std::chrono;

// -------------------------- 基础工具 & 日志 --------------------------
static uint64_t now_ms() {
  return duration_cast<milliseconds>(steady_clock::now().time_since_epoch())
      .count();
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
static const size_t SMALL_BLOCK = 512;   // 小块：协议头或小包
static const size_t LARGE_BLOCK = 4096;  // 大块：主体或大包
static const int MAX_ACCEPT_BATCH = 64;  // 每次 accept 最多尝试次数
static const size_t OUT_RING_CAP = 1024; // 每连接出站环容量
static const uint64_t DEFAULT_IDLE_MS = 60 * 1000;
static const uint64_t DEFAULT_ACTIVE_MS = 5 * 60 * 1000;

// -------------------------- Metrics --------------------------
struct Metrics {
  std::atomic<uint64_t> accepted{0}, closed{0}, rx_bytes{0}, tx_bytes{0},
      rx_pkts{0}, tx_pkts{0}, drops{0}, parse_errors{0}, timeouts{0};
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
                   "server_timeouts %llu\n",
                   (unsigned long long)g_metrics.accepted.load(),
                   (unsigned long long)g_metrics.closed.load(),
                   (unsigned long long)g_metrics.rx_bytes.load(),
                   (unsigned long long)g_metrics.tx_bytes.load(),
                   (unsigned long long)g_metrics.rx_pkts.load(),
                   (unsigned long long)g_metrics.tx_pkts.load(),
                   (unsigned long long)g_metrics.drops.load(),
                   (unsigned long long)g_metrics.parse_errors.load(),
                   (unsigned long long)g_metrics.timeouts.load());
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

// -------------------------- BufferBlock & 双层 BufferPool
// --------------------------
struct BufferBlock {
  std::atomic<int> refcount;
  BufferBlock *next;
  size_t cap;
  alignas(64) uint8_t *data;
};

class BufferPoolBase {
public:
  BufferPoolBase(size_t block_size, size_t total_blocks) : bs_(block_size) {
    storage_.resize(total_blocks);
    backing_.resize(total_blocks * block_size);
    for (size_t i = 0; i < total_blocks; ++i)
      storage_.emplace_back(std::make_unique<BufferBlock>());

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
  void release_ref(BufferBlock *b) {
    int prev = b->refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == 1) {
      BufferBlock *head = freelist_.load(std::memory_order_relaxed);
      do {
        b->next = head;
      } while (!freelist_.compare_exchange_weak(
          head, b, std::memory_order_release, std::memory_order_relaxed));
    }
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
      : small_(SMALL_BLOCK, small_blocks), large_(LARGE_BLOCK, large_blocks) {}
  // 根据期望大小尝试分配：优先小块，回退到大块
  BufferBlock *acquire(size_t expect) {
    if (expect <= small_.block_size()) {
      if (auto b = small_.acquire())
        return b;
    }
    return large_.acquire();
  }
  void retain(BufferBlock *b) {
    (b->cap == SMALL_BLOCK ? small_ : large_).retain(b);
  }
  void release_ref(BufferBlock *b) {
    (b->cap == SMALL_BLOCK ? small_ : large_).release_ref(b);
  }

private:
  BufferPoolBase small_;
  BufferPoolBase large_;
};

// -------------------------- MPMC 环形队列（用于工作队列 / timingwheel）
// --------------------------
template <typename T> class MPMCRing {
public:
  explicit MPMCRing(size_t cap_pow2) : size_(cap_pow2), mask_(cap_pow2 - 1) {
    assert((cap_pow2 & (cap_pow2 - 1)) == 0);
    entries_.resize(size_);
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
  alignas(64) std::atomic<size_t> head_, tail_;
};

template <typename T> class WorkQueueWrapper {
public:
  WorkQueueWrapper() : q_(1 << 12) {}
  bool push(const T &v) { return q_.enqueue(v); }
  bool pop(T &o) { return q_.dequeue(o); }

private:
  MPMCRing<T> q_;
};
template <typename T> using WorkQueue = WorkQueueWrapper<T>;

// -------------------------- 协议头（示例） --------------------------
struct PacketHeader {
  uint8_t magic;
  uint32_t body_len;
  uint8_t endian;
  uint32_t hdr_crc;
};

// -------------------------- RingBuffer（零拷贝入站，支持双层块）
// --------------------------
class RingBuffer {
public:
  RingBuffer(DualBufferPool &pool)
      : pool_(pool), head_(nullptr), tail_(nullptr), head_off_(0),
        tail_off_(0) {
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
  // 获取可写区域（hint 参数决定分配小/大块）
  iovec writable_region(size_t hint = SMALL_BLOCK) {
    ensure_tail(hint);
    return iovec{tail_->data + tail_off_, tail_->cap - tail_off_ - 1};
  }
  // 标记已写入 n 字节
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
      size_t avail =
          (head_ == tail_) ? (tail_off_ - head_off_) : (head_->cap - head_off_);
      if (avail > remain) {
        head_off_ += remain;
        return;
      }
      remain -= avail;
      BufferBlock *old = head_;
      head_ = old->next;
      pool_.release_ref(old);
      head_off_ = 0;
      if (!head_) {
        tail_ = nullptr;
        tail_off_ = 0;
        break;
      }
    }
  }
  size_t readable_bytes() const {
    if (!head_)
      return 0;
    if (head_ == tail_)
      return (tail_off_ >= head_off_) ? (tail_off_ - head_off_) : 0;
    size_t cnt = 0;
    BufferBlock *cur = head_;
    size_t off = head_off_;
    while (cur) {
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
  struct StealEntry {
    BufferBlock *blk;
    size_t offset;
    size_t len;
  };
  // steal_body_after: 在 header_len 之后“窃取” body_len 的块，返回多个
  // StealEntry（零拷贝）
  bool steal_body_after(size_t header_len, size_t body_len,
                        std::vector<StealEntry> &out) {
    if (readable_bytes() < header_len + body_len)
      return false;
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
    size_t remain = body_len;
    std::vector<BufferBlock *> keep;
    while (remain > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off) : (cur->cap - off);
      size_t take = std::min(avail, remain);
      out.push_back({cur, off, take});
      if (keep.empty() || keep.back() != cur)
        keep.push_back(cur);
      remain -= take;
      off += take;
      if (off >= cur->cap) {
        cur = cur->next;
        off = 0;
      }
    }
    for (BufferBlock *b : keep)
      pool_.retain(b);
    consume(header_len + body_len);
    return true;
  }

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
      tail_->next = b;
      tail_ = b;
    }
  }
  void ensure_tail(size_t hint) {
    if (!tail_ || tail_off_ >= tail_->cap - 1) {
      append_block(hint);
      tail_off_ = 0;
    }
  }
};

// -------------------------- OutEntry & Connection --------------------------
struct OutEntry {
  BufferBlock *blk;
  size_t offset;
  size_t len;
};

class Connection {
public:
  int fd;
  RingBuffer *rx;
  std::vector<OutEntry> out_ring;
  size_t out_cap;
  size_t out_head;
  size_t out_tail;
  uint64_t last_active_ms;
  Connection(int fd_, RingBuffer *rb, size_t cap)
      : fd(fd_), rx(rb), out_ring(cap), out_cap(cap), out_head(0), out_tail(0),
        last_active_ms(now_ms()) {}
  bool out_push(const OutEntry &e) {
    size_t n = (out_tail + 1) % out_cap;
    if (n == out_head)
      return false;
    out_ring[out_tail] = e;
    out_tail = n;
    return true;
  }
  bool out_empty() const { return out_head == out_tail; }
  // 填充 iovec，并返回被 peek 的条目下标（用于 advance）
  size_t out_peek(iovec *iovs, size_t max, std::vector<size_t> &idxs) {
    size_t cnt = 0, cur = out_head;
    while (cur != out_tail && cnt < max) {
      const OutEntry &e = out_ring[cur];
      iovs[cnt] = iovec{e.blk->data + e.offset, e.len};
      idxs.push_back(cur);
      ++cnt;
      cur = (cur + 1) % out_cap;
    }
    return cnt;
  }
  void out_advance(size_t bytes_written, DualBufferPool &pool) {
    size_t rem = bytes_written;
    while (rem > 0 && out_head != out_tail) {
      OutEntry &e = out_ring[out_head];
      if (rem >= e.len) {
        rem -= e.len;
        BufferBlock *b = e.blk;
        e.blk = nullptr;
        e.offset = e.len = 0;
        pool.release_ref(b);
        out_head = (out_head + 1) % out_cap;
      } else {
        e.offset += rem;
        e.len -= rem;
        rem = 0;
        break;
      }
    }
  }
};

// -------------------------- ConnectionPool --------------------------
class ConnectionPool {
public:
  ConnectionPool(size_t max_conn, DualBufferPool &pool)
      : max_conn_(max_conn), pool_(pool) {}
  bool admit(int fd, std::unique_ptr<Connection> &out_conn) {
    for (;;) {
      size_t cur = active_.load(std::memory_order_relaxed);
      if (cur >= max_conn_)
        return false;
      if (active_.compare_exchange_strong(cur, cur + 1))
        break;
    }
    RingBuffer *rb = new RingBuffer(pool_);
    out_conn.reset(new Connection(fd, rb, OUT_RING_CAP));
    return true;
  }
  void release(std::unique_ptr<Connection> &conn) {
    if (!conn)
      return;
    delete conn->rx;
    conn->rx = nullptr;
    ::close(conn->fd);
    conn.reset();
    active_.fetch_sub(1);
  }

private:
  size_t max_conn_;
  DualBufferPool &pool_;
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
      : tick_ms_(tick_ms), slots_(normalize_pow2(slots)),
        slot_mask_(slots_ - 1), initialized_(false), last_slot_index_(0) {
    slots_q_.reserve(slots_);
    for (size_t i = 0; i < slots_; ++i)
      slots_q_.emplace_back(std::make_unique<MPMCRing<Entry>>(1 << 14));
  }
  inline void add(int fd, uint64_t expire_ms) {
    Entry e{fd, expire_ms};
    size_t idx = slot_index(expire_ms);
    for (int i = 0; i < 64; ++i) {
      if (slots_q_[idx]->enqueue(e))
        return;
      std::this_thread::yield();
    }
    g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
  }
  template <typename F> void tick(uint64_t now_ms, F on_timeout) {
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
  template <typename F> void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
    Entry e;
    while (slots_q_[idx]->dequeue(e)) {
      if (e.expire_ms <= now_ms)
        on_timeout(e.fd);
      else
        add(e.fd, e.expire_ms);
    }
  }
  template <typename F>
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
  std::vector<OutEntry> entries;
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
business_worker_echo(int fd, std::vector<RingBuffer::StealEntry> &stolen,
                     TaskQueue &reactor_queue) {
  // 示例业务：echo（零拷贝）
  ResponseTask t;
  t.fd = fd;
  t.entries.reserve(stolen.size());
  for (auto &s : stolen) {
    t.entries.push_back({s.blk, s.offset, s.len});
  }
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
    int mfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
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
    }

    std::vector<epoll_event> evs(4096);
    std::unordered_map<int, std::unique_ptr<Connection>> conns;

    while (!global_stop_.load()) {
      // 先处理 worker 发回的响应，减少尾延迟
      process_incoming_tasks(conns);

      int n = epoll_wait(epfd_, evs.data(), (int)evs.size(), 1000);
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
          auto &conn = it->second;
          if (events & (EPOLLHUP | EPOLLERR | EPOLLRDHUP)) {
            close_conn(conn);
            continue;
          }
          if (events & EPOLLIN)
            on_readable(conn); // [ET-CRITICAL] read 循环到 EAGAIN
          if (events & EPOLLOUT)
            on_writable(conn); // [ET-CRITICAL] write 循环到 EAGAIN 或队列空
          if (tnow - conn->last_active_ms > idle_ms_) {
            g_metrics.timeouts++;
            close_conn(conn);
          }
        }
      }
    }

    for (auto &p : conns)
      cpool_.release(p.second);
    if (metrics_fd_ >= 0)
      close(metrics_fd_);
    close(epfd_);
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
  void
  accept_loop(std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    for (int i = 0; i < MAX_ACCEPT_BATCH; ++i) {
      sockaddr_in in{};
      socklen_t inlen = sizeof(in);
      int cfd = accept4(listen_fd_, (sockaddr *)&in, &inlen,
                        SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (cfd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          break;
        if (errno == EINTR)
          continue;
        else
          break;
      }
      set_tcp_options(cfd);
      std::unique_ptr<Connection> connptr;
      if (!cpool_.admit(cfd, connptr)) {
        ::close(cfd);
        continue;
      }
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
      ev.data.fd = cfd;
      if (epoll_ctl(epfd_, EPOLL_CTL_ADD, cfd, &ev) < 0) {
        ::close(cfd);
        cpool_.release(connptr);
        continue;
      }
      conns[cfd] = std::move(connptr);
      g_metrics.accepted++;
      wheel_.add(cfd, now_ms() + active_ms_);
    }
  }

  // [ET-CRITICAL] read 必须拉空直到 EAGAIN
  void on_readable(std::unique_ptr<Connection> &conn) {
    for (;;) {
      iovec w = conn->rx->writable_region(SMALL_BLOCK); // 先用小块读头
      if (w.iov_len == 0)
        break;
      ssize_t n = ::read(conn->fd, w.iov_base, w.iov_len);
      if (n > 0) {
        conn->rx->produce((size_t)n);
        g_metrics.rx_bytes += (uint64_t)n;
        conn->last_active_ms = now_ms();
        continue;
      }
      if (n == 0) {
        throw_close(conn);
        return;
      }
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          break;
        if (errno == EINTR)
          continue;
        throw_close(conn);
        return;
      }
    }

    const size_t H = 1 + 4 + 1 + 4; // magic + body_len + endian + hdr_crc
    while (true) {
      if (conn->rx->readable_bytes() < H)
        break;
      uint8_t hdrbuf[10];
      size_t got = conn->rx->peek_bytes(hdrbuf, H);
      if (got < H)
        break;
      PacketHeader hdr;
      hdr.magic = hdrbuf[0];
      uint32_t bl;
      memcpy(&bl, hdrbuf + 1, 4);
      hdr.endian = hdrbuf[5];
      memcpy(&hdr.hdr_crc, hdrbuf + 6, 4);
      uint32_t crc = crc32_calc(hdrbuf, 6);
      if (hdr.magic != 0x7e || hdr.hdr_crc != crc) {
        g_metrics.parse_errors++;
        throw_close(conn);
        return;
      }
      hdr.body_len = hdr.endian ? bswap32_u32(bl) : bl;
      const uint32_t MAX_BODY = 16 * 1024 * 1024;
      if (hdr.body_len > MAX_BODY) {
        g_metrics.parse_errors++;
        throw_close(conn);
        return;
      }
      if (conn->rx->readable_bytes() < H + hdr.body_len)
        break;

      // consume header
      conn->rx->consume(H);

      // 读体阶段提示使用大块以提升吞吐
      (void)conn->rx->writable_region(std::min<size_t>(
          LARGE_BLOCK, std::max<size_t>(SMALL_BLOCK, hdr.body_len)));

      // steal body 零拷贝
      std::vector<RingBuffer::StealEntry> stolen;
      if (!conn->rx->steal_body_after(0, hdr.body_len, stolen)) {
        g_metrics.drops++;
        break;
      }
      g_metrics.rx_pkts++;

      // 把工作投递给 worker（示例：echo）
      auto job = [fd = conn->fd, stolen = std::move(stolen), this]() mutable {
        business_worker_echo(
            fd, const_cast<std::vector<RingBuffer::StealEntry> &>(stolen),
            taskq_);
      };
      workers_.submit(job);
    }
  }

  // [ET-CRITICAL] write 必须写到 EAGAIN 或队列空。若队列空则从 epoll 中移除
  // EPOLLOUT 避免空转。
  void on_writable(std::unique_ptr<Connection> &conn) {
    while (!conn->out_empty()) {
      iovec iovs[OUT_RING_CAP];
      std::vector<size_t> idxs;
      idxs.reserve(OUT_RING_CAP);
      size_t cnt = conn->out_peek(iovs, OUT_RING_CAP, idxs);
      if (cnt == 0)
        break;
      ssize_t n = writev(conn->fd, iovs, (int)cnt);
      if (n > 0) {
        g_metrics.tx_bytes += (uint64_t)n;
        g_metrics.tx_pkts++;
        conn->out_advance((size_t)n, pool_);
        continue;
      }
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
          break;
        throw_close(conn);
        return;
      }
    }
    if (conn->out_empty()) {
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLRDHUP | EPOLLET;
      ev.data.fd = conn->fd;
      epoll_ctl(epfd_, EPOLL_CTL_MOD, conn->fd, &ev);
    }
  }

  void on_timeout(int fd,
                  std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    auto it = conns.find(fd);
    if (it == conns.end())
      return;
    if (now_ms() - it->second->last_active_ms > idle_ms_) {
      g_metrics.timeouts++;
      close_conn(it->second);
    } else {
      wheel_.add(fd, now_ms() + active_ms_);
    }
  }

  void process_incoming_tasks(
      std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    ResponseTask t;
    while (taskq_.dequeue(t)) {
      auto it = conns.find(t.fd);
      if (it == conns.end()) {
        for (auto &e : t.entries)
          pool_.release_ref(e.blk);
        continue;
      }
      auto &conn = it->second;
      bool ok_all = true;
      for (auto &e : t.entries) {
        if (!conn->out_push(e)) {
          ok_all = false;
          break;
        }
      }
      if (!ok_all) {
        for (auto &e : t.entries)
          pool_.release_ref(e.blk);
        g_metrics.drops++;
      } else {
        epoll_event ev{};
        ev.events = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;
        ev.data.fd = conn->fd;
        epoll_ctl(epfd_, EPOLL_CTL_MOD, conn->fd, &ev);
      }
    }
  }

  void serve_metrics(int mfd) {
    for (;;) {
      sockaddr_in cli;
      socklen_t len = sizeof cli;
      int c =
          accept4(mfd, (sockaddr *)&cli, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
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
        int hn =
            snprintf(hdr, sizeof hdr,
                     "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\nContent-Type: "
                     "text/plain; version=0.0.4\r\nConnection: close\r\n\r\n",
                     body.size());
        send(c, hdr, hn, 0);
        send(c, body.data(), body.size(), 0);
      } else {
        const char *r =
            "HTTP/1.1 404\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        send(c, r, strlen(r), 0);
      }
      close(c);
    }
  }

  void close_conn(std::unique_ptr<Connection> &conn) {
    if (!conn)
      return;
    g_metrics.closed++;
    cpool_.release(conn);
  }
  void throw_close(std::unique_ptr<Connection> &conn) { close_conn(conn); }
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
  size_t small_blocks = (size_t)ncpu * 64 * 1024; // 可按内存和连接数调节
  size_t large_blocks = (size_t)ncpu * 32 * 1024;

  LOG_INFO("ET-opt server starting on port %u with %d CPUs, small_blocks=%zu, "
           "large_blocks=%zu",
           port, ncpu, small_blocks, large_blocks);

  DualBufferPool pool(small_blocks, large_blocks);
  size_t max_conn = 1000000;
  ConnectionPool cpool(max_conn, pool);
  size_t worker_threads = std::max(1, ncpu * 1);
  WorkerPool workers(worker_threads);

  int listen_fd =
      socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
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
      std::this_thread::sleep_for(std::chrono::seconds(1));
      auto now = steady_clock::now();
      double s = duration_cast<duration<double>>(now - last).count();
      last = now;
      uint64_t rx = g_metrics.rx_bytes.load(), tx = g_metrics.tx_bytes.load();
      double rxrate = (rx - last_rx) / s, txrate = (tx - last_tx) / s;
      last_rx = rx;
      last_tx = tx;
      fprintf(stderr,
              "[metrics] acc=%llu cls=%llu rx=%llu tx=%llu rx/s=%.0fB "
              "tx/s=%.0fB pkts(rx=%llu tx=%llu) drop=%llu err=%llu to=%llu\n",
              (unsigned long long)g_metrics.accepted.load(),
              (unsigned long long)g_metrics.closed.load(),
              (unsigned long long)rx, (unsigned long long)tx, rxrate, txrate,
              (unsigned long long)g_metrics.rx_pkts.load(),
              (unsigned long long)g_metrics.tx_pkts.load(),
              (unsigned long long)g_metrics.drops.load(),
              (unsigned long long)g_metrics.parse_errors.load(),
              (unsigned long long)g_metrics.timeouts.load());
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
