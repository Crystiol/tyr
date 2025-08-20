// epoll_server_prod_zero_copy_lt_final.cpp
// 高性能零拷贝 Epoll 服务器（单文件）
//
// 主要特性：
// - 零拷贝 BufferPool（固定块 + 引用计数）
// - RingBuffer（入站零拷贝 + body 窃取）
// - WorkerPool（业务线程池）
// - ConnectionPool（连接限制与资源释放）
// - Reactor（epoll + timerfd + metrics）
// - TimingWheel：无锁单层 MPMC 时间轮（替换锁实现）
// - 中文注释标出关键并发/内存/零拷贝要点
//
// 编译：g++ -std=c++11 -O2 epoll_server_prod_zero_copy_lt_final.cpp -lpthread -o server

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

static inline void die(const char *s) {
  perror(s);
  exit(1);
}

// -------------------------- 可配置参数 --------------------------
static const size_t BUFFER_BLOCK_SIZE = 4096;
static const size_t DEFAULT_BLOCKS_PER_CPU = 64 * 1024; // 根据内存/连接量调节
static const size_t OUT_RING_CAP = 1024;
static const uint64_t DEFAULT_IDLE_MS = 60 * 1000;
static const uint64_t DEFAULT_ACTIVE_MS = 5 * 60 * 1000;

struct Metrics {
    std::atomic<uint64_t> accepted{0}, closed{0}, rx_bytes{0}, tx_bytes{0},
        rx_pkts{0}, tx_pkts{0}, drops{0}, parse_errors{0}, timeouts{0};
};

// -------------------------- Metrics --------------------------
struct Metrics g_metrics;

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

// -------------------------- CRC32 --------------------------
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

// -------------------------- 简单 MPMC 环（用于内部队列） --------------------------
template <typename T> class MPMCRing {
public:
  explicit MPMCRing(size_t cap_pow2) : size_(cap_pow2), mask_(cap_pow2 - 1) {
    // cap_pow2 必须是 2 的幂（为了 & 取模）
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

// -------------------------- BufferPool（固定大小块，带引用计数） --------------------------
/*
  设计要点：
  - BufferBlock 大小固定（BUFFER_BLOCK_SIZE）
  - 内存以数组方式预分配，避免频繁 new/free
  - 使用无锁 freelist（单向链表 + CAS），acquire/return 都是无锁的
  - refcount 用于多个消费者共享同一 block（零拷贝传递）
*/
struct BufferBlock {
  std::atomic<int> refcount;
  BufferBlock *next;
  alignas(64) uint8_t data[BUFFER_BLOCK_SIZE];
};

class BufferPool {
public:
  explicit BufferPool(size_t total_blocks) {
    storage_.reserve(total_blocks);
    for (size_t i = 0; i < total_blocks; ++i)
      storage_.emplace_back(std::make_unique<BufferBlock>());

    // 构建 freelist
    for (size_t i = 0; i + 1 < total_blocks; ++i)
      storage_[i]->next = storage_[i + 1].get();
    storage_.back()->next = nullptr;
    freelist_.store(storage_[0].get(), std::memory_order_relaxed);
  }

  // 返回一个持有 refcount=1 的块，失败返回 nullptr
  BufferBlock *acquire() {
    BufferBlock* h = freelist_.load(std::memory_order_acquire);
    while (h) {
      BufferBlock* nxt = h->next;
      if (freelist_.compare_exchange_weak(h, nxt,
                                         std::memory_order_acq_rel)) {
        h->next = nullptr;
        h->refcount.store(1, std::memory_order_release);
        return h;
                                         }
    }
    return nullptr;
  }

  // 增加引用
  void retain(BufferBlock *b) { b->refcount.fetch_add(1, std::memory_order_acq_rel); }

  // 释放引用：当 refcount 从 1 -> 0 时，回收到 freelist
  void release_ref(BufferBlock *b) {
    int prev = b->refcount.fetch_sub(1, std::memory_order_acq_rel);
    if (prev == 1) {
      BufferBlock* head = freelist_.load(std::memory_order_relaxed);
      do {
        b->next = head;
      } while (!freelist_.compare_exchange_weak(
          head, b, std::memory_order_release, std::memory_order_relaxed));
    }
  }

private:
  std::vector<std::unique_ptr<BufferBlock>> storage_;
  std::atomic<BufferBlock *> freelist_;
};

// -------------------------- RingBuffer（零拷贝入站） --------------------------
/*
  设计要点：
  - RingBuffer 持有若干 BufferBlock 的链表 head->...->tail
  - 生产者：writable_region()+produce(n)
  - 消费者：peek_bytes/consume
  - 零拷贝：steal_body_after() 对 body 增加引用，随后 consume 掉 ring 中对应字节
*/
class RingBuffer {
public:
  RingBuffer(BufferPool &pool)
      : pool_(pool), head_(nullptr), tail_(nullptr), head_off_(0), tail_off_(0) {
    append_block();
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

  // 返回可写区域（不跨块）
  iovec writable_region() {
    ensure_tail();
    return iovec{tail_->data + tail_off_, BUFFER_BLOCK_SIZE - tail_off_ - 1};
  }

  // 生产者在写入后调用，n 为写入字节数
  void produce(size_t n) {
    tail_off_ += n;
    if (tail_off_ >= BUFFER_BLOCK_SIZE - 1) {
      append_block();
      tail_off_ = 0;
    }
  }

  // 从 head 开始 peek 最多 n 字节到 dst，但不消费 ring
  size_t peek_bytes(uint8_t *dst, size_t n) {
    if (!head_)
      return 0;
    size_t copied = 0;
    BufferBlock *cur = head_;
    size_t off = head_off_;
    size_t remain = n;
    while (remain > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off) : (BUFFER_BLOCK_SIZE - off);
      if (avail == 0)
        break;
      size_t take = std::min(avail, remain);
      memcpy(dst + copied, cur->data + off, take);
      copied += take;
      remain -= take;
      off += take;
      if (off >= BUFFER_BLOCK_SIZE) {
        cur = cur->next;
        off = 0;
      }
    }
    return copied;
  }

  // 消费 n 字节，并释放完整消费掉的 blocks 的引用
  void consume(size_t n) {
    size_t remain = n;
    while (remain > 0 && head_) {
      size_t avail = (head_ == tail_) ? (tail_off_ - head_off_) : (BUFFER_BLOCK_SIZE - head_off_);
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

  // 可读字节数（跨块计算）
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
      cnt += BUFFER_BLOCK_SIZE - off;
      cur = cur->next;
      off = 0;
    }
    return cnt;
  }

  // Steal body：在已 consume header 的前提下，为 body 创建引用列表 out（零拷贝）
  struct StealEntry {
    BufferBlock *blk;
    size_t offset;
    size_t len;
  };
  bool steal_body_after(size_t header_len, size_t body_len, std::vector<StealEntry> &out) {
    if (readable_bytes() < header_len + body_len)
      return false;
    BufferBlock *cur = head_;
    size_t off = head_off_;
    size_t skip = header_len;
    while (skip > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off) : (BUFFER_BLOCK_SIZE - off);
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
    std::vector<BufferBlock *> to_retain;
    while (remain > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off) : (BUFFER_BLOCK_SIZE - off);
      size_t take = std::min(avail, remain);
      out.push_back({cur, off, take});
      if (to_retain.empty() || to_retain.back() != cur)
        to_retain.push_back(cur);
      remain -= take;
      off += take;
      if (off >= BUFFER_BLOCK_SIZE) {
        cur = cur->next;
        off = 0;
      }
    }
    for (BufferBlock *b : to_retain) pool_.retain(b);
    consume(header_len + body_len); // ring 前进（释放 ring 持有的引用）
    return true;
  }

private:
  BufferPool &pool_;
  BufferBlock *head_;
  BufferBlock *tail_;
  size_t head_off_, tail_off_;

  void append_block() {
    BufferBlock *b = pool_.acquire();
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
  void ensure_tail() {
    if (!tail_) {
      append_block();
      return;
    }
    if (tail_off_ >= BUFFER_BLOCK_SIZE - 1) {
      append_block();
      tail_off_ = 0;
    }
  }
};

// -------------------------- 无锁 TimingWheel（替换原锁实现） --------------------------
/*
  设计目标：
  - 每个槽 slot 使用 MPMC 无锁环（MPMCRing<Entry>），add()/tick() 无需全局锁。
  - tick() 将 last_slot 推进到 now_ms 所在槽，逐槽 drain，避免“集中抖动”。
  - 未到期项轻量回插（重新定位槽）；槽满时短暂自旋并让出，极端下计为 drops。
*/
class TimingWheel {
public:
  struct Entry { int fd; uint64_t expire_ms; };

  // tick_ms: 槽粒度；slots: 槽个数（将提升为2的幂）；slot_cap: 单槽容量
  explicit TimingWheel(uint64_t tick_ms, size_t slots, size_t slot_cap = (1u<<14))
      : tick_ms_(tick_ms), slots_(normalize_pow2(slots)),
        slot_mask_(slots_ - 1), initialized_(false), last_slot_index_(0) {
    slots_q_.reserve(slots_);
    for (size_t i = 0; i < slots_; ++i)
      slots_q_.emplace_back(std::make_unique<MPMCRing<Entry>>(slot_cap));
  }

  // 无锁 add；槽满时短暂自旋，让出 CPU；极端情况下计入 drops（避免阻塞）
  inline void add(int fd, uint64_t expire_ms) {
    Entry e{fd, expire_ms};
    size_t idx = slot_index(expire_ms);
    for (int i = 0; i < 64; ++i) {
      if (slots_q_[idx]->enqueue(e)) return;
      std::this_thread::yield();
    }
    g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
  }

  // 推进到 now_ms 相应槽位，并处理到期/重插
  template <typename F>
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
    drain_light(cur, now_ms, on_timeout); // 可选温和刷新
  }

private:
  template <typename F>
  void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
    Entry e;
    while (slots_q_[idx]->dequeue(e)) {
      if (e.expire_ms <= now_ms) on_timeout(e.fd);
      else add(e.fd, e.expire_ms);
    }
  }
  template <typename F>
  void drain_light(size_t idx, uint64_t now_ms, F &on_timeout) {
    Entry e;
    for (int k = 0; k < 64; ++k) {
      if (!slots_q_[idx]->dequeue(e)) break;
      if (e.expire_ms <= now_ms) on_timeout(e.fd);
      else add(e.fd, e.expire_ms);
    }
  }
  static size_t normalize_pow2(size_t x) { size_t p=1; while(p<x) p<<=1; return p; }
  inline size_t slot_index(uint64_t expire_ms) const {
    return ((expire_ms / tick_ms_) & slot_mask_);
  }

  const uint64_t tick_ms_;
  const size_t   slots_, slot_mask_;
  std::vector<std::unique_ptr<MPMCRing<Entry>>> slots_q_;
  std::atomic<bool>   initialized_;
  std::atomic<size_t> last_slot_index_;
};

// -------------------------- WorkQueue (可替换为 TBB) --------------------------
template <typename T> class WorkQueueWrapper {
public:
  WorkQueueWrapper() : q_(1 << 12) {}
  bool push(const T &v) { return q_.enqueue(v); }
  bool pop(T &o) { return q_.dequeue(o); }

private:
  MPMCRing<T> q_;
};
template <typename T> using WorkQueue = WorkQueueWrapper<T>;

// -------------------------- 协议定义（示例） --------------------------
/*
 Format:
   magic (1 byte) = 0x7e
   body_len (4 bytes) (network/host conditional)
   endian (1 byte) : 0 => little, 1 => big (means body_len is network byte order)
   hdr_crc (4 bytes) : CRC32 of first 6 bytes (magic + body_len + endian)
   body (body_len bytes)
*/
struct PacketHeader {
  uint8_t magic;
  uint32_t body_len;
  uint8_t endian;
  uint32_t hdr_crc;
};

// -------------------------- 连接与连接池 --------------------------
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
      return false; // 满
    out_ring[out_tail] = e;
    out_tail = n;
    return true;
  }
  bool out_empty() const { return out_head == out_tail; }

  size_t out_peek(iovec *iovs, size_t max, std::vector<size_t> &idxs) {
    size_t cnt = 0;
    size_t cur = out_head;
    while (cur != out_tail && cnt < max) {
      const OutEntry &e = out_ring[cur];
      iovs[cnt] = iovec{e.blk->data + e.offset, e.len};
      idxs.push_back(cur);
      ++cnt;
      cur = (cur + 1) % out_cap;
    }
    return cnt;
  }

  void out_advance(size_t bytes_written, BufferPool &pool) {
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

class ConnectionPool {
public:
  ConnectionPool(size_t max_conn, BufferPool &pool) : max_conn_(max_conn), pool_(pool) {}

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
    delete conn->rx;     // RingBuffer 析构释放剩余块
    conn->rx = nullptr;
    ::close(conn->fd);
    conn.reset();
    active_.fetch_sub(1);
  }

private:
  size_t max_conn_;
  BufferPool &pool_;
  std::atomic<size_t> active_{0};
};

// -------------------------- TaskQueue（worker -> reactor） --------------------------
struct ResponseTask {
  int fd;
  std::vector<OutEntry> entries;
};

class TaskQueue {
public:
  TaskQueue() : q_(1 << 14) {}
  bool enqueue(const ResponseTask &t) { return q_.enqueue(t); }
  bool dequeue(ResponseTask &out) { return q_.dequeue(out); }

private:
  MPMCRing<ResponseTask> q_;
};

// -------------------------- 业务逻辑（示例：零拷贝 echo） --------------------------
void business_worker_echo(int fd, std::vector<RingBuffer::StealEntry> &stolen,
                          TaskQueue &reactor_queue) {
  ResponseTask t;
  t.fd = fd;
  t.entries.reserve(stolen.size());
  for (auto &s : stolen) {
    OutEntry e;
    e.blk = s.blk;
    e.offset = s.offset;
    e.len = s.len;
    t.entries.push_back(e);
  }
  while (!reactor_queue.enqueue(t))
    std::this_thread::yield();
}

// -------------------------- WorkerPool（业务线程池） --------------------------
class WorkerPool {
public:
  using Job = std::function<void()>;
  WorkerPool(size_t n) : stop_(false) {
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

// -------------------------- Reactor（IO 线程） --------------------------
class Reactor {
public:
  Reactor(int cpu_id, int listen_fd, BufferPool &pool, ConnectionPool &cpool,
          WorkerPool &workers, uint64_t idle_ms, uint64_t active_ms)
      : cpu_id_(cpu_id), listen_fd_(listen_fd), pool_(pool), cpool_(cpool),
        workers_(workers), idle_ms_(idle_ms), active_ms_(active_ms),
        taskq_(), wheel_(100, 1024) {}

  void run() {
    pin_cpu(cpu_id_);
    epfd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0)
      die("epoll_create1");

    // 注册 listen fd（LT 模式）——此处附带 EPOLLET 以减少唤醒（可按需去掉 ET 标志）
    epoll_event ev{};
    ev.events = EPOLLIN | EPOLLET;
    ev.data.fd = listen_fd_;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, listen_fd_, &ev) < 0)
      die("epoll_ctl listen");

    // timerfd 用于定期触发 timing wheel tick
    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (tfd < 0)
      die("timerfd_create");
    itimerspec its{};
    its.it_interval = {0, 100 * 1000 * 1000}; // 100ms
    its.it_value = its.it_interval;
    timerfd_settime(tfd, 0, &its, nullptr);
    epoll_event tev{};
    tev.events = EPOLLIN;
    tev.data.fd = tfd;
    if (epoll_ctl(epfd_, EPOLL_CTL_ADD, tfd, &tev) < 0)
      die("epoll_ctl timer");

    // metrics socket：绑定 loopback
    int mfd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (mfd < 0)
      die("metrics socket");
    int yes = 1;
    setsockopt(mfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof yes);
    sockaddr_in ma{};
    ma.sin_family = AF_INET;
    ma.sin_port = htons(metrics_port_);
    ma.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (bind(mfd, (sockaddr *)&ma, sizeof ma) < 0) {
      close(mfd);
      LOG_ERROR("metrics bind failed, metrics disabled");
    } else {
      if (listen(mfd, 64) < 0) {
        close(mfd);
        LOG_ERROR("metrics listen failed");
      } else {
        epoll_event mev{};
        mev.events = EPOLLIN;
        mev.data.fd = mfd;
        if (epoll_ctl(epfd_, EPOLL_CTL_ADD, mfd, &mev) < 0) {
          LOG_ERROR("failed to register metrics fd");
          close(mfd);
        } else {
          metrics_fd_ = mfd;
        }
      }
    }

    std::vector<epoll_event> evs(4096);
    std::unordered_map<int, std::unique_ptr<Connection>> conns;

    while (!global_stop_.load()) {
      // 先处理 worker 发回的响应任务（降低尾延迟）
      process_incoming_tasks(conns);

      int n = epoll_wait(epfd_, evs.data(), (int)evs.size(), 1000);
      if (n < 0) {
        if (errno == EINTR)
          continue;
        die("epoll_wait");
      }
      uint64_t now = now_ms();
      for (int i = 0; i < n; ++i) {
        int fd = evs[i].data.fd;
        uint32_t events = evs[i].events;
        if (fd == listen_fd_) {
          accept_loop(conns);
        } else if (fd == tfd) {
          uint64_t exp;
          ssize_t r = read(tfd, &exp, sizeof(exp)); (void)r;
          wheel_.tick(now, [&](int xfd) { on_timeout(xfd, conns); });
        } else if (fd == metrics_fd_) {
          serve_metrics(fd);
        } else {
          auto it = conns.find(fd);
          if (it == conns.end())
            continue;
          auto &conn = it->second;
          if (events & (EPOLLHUP | EPOLLERR)) {
            close_conn(it->second);
            continue;
          }
          if (events & EPOLLIN) {
            on_readable(conn);
          }
          if (events & EPOLLOUT) {
            on_writable(conn);
          }
          if (now - conn->last_active_ms > idle_ms_) {
            g_metrics.timeouts++;
            close_conn(it->second);
          }
        }
      }
    }

    // 关闭阶段：释放所有连接
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
  BufferPool &pool_;
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

  void accept_loop(std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    for (;;) {
      sockaddr_in in{};
      socklen_t inlen = sizeof(in);
      int cfd = accept4(listen_fd_, (sockaddr *)&in, &inlen, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (cfd < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        if (errno == EINTR) continue;
        else break;
      }
      set_tcp_options(cfd);
      std::unique_ptr<Connection> connptr;
      if (!cpool_.admit(cfd, connptr)) {
        ::close(cfd);
        continue;
      }
      epoll_event ev{};
      ev.events = EPOLLIN | EPOLLET; // 有数据要发时再 MOD 加 EPOLLOUT
      ev.data.fd = cfd;
      if (epoll_ctl(epfd_, EPOLL_CTL_ADD, cfd, &ev) < 0) {
        ::close(cfd);
        cpool_.release(connptr);
        continue;
      }
      conns[cfd] = std::move(connptr);
      g_metrics.accepted++;
      wheel_.add(cfd, now_ms() + active_ms_); // 加入时间轮
    }
  }

  void on_readable(std::unique_ptr<Connection> &conn) {
    for (;;) {
      iovec w = conn->rx->writable_region();
      if (w.iov_len == 0)
        break;
      ssize_t n = ::read(conn->fd, w.iov_base, w.iov_len);
      if (n > 0) {
        conn->rx->produce((size_t)n);
        g_metrics.rx_bytes += (uint64_t)n;
        conn->last_active_ms = now_ms();
      }
      if (n == 0) { throw_close(conn); return; }
      if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        if (errno == EINTR) continue;
        throw_close(conn); return;
      }
    }

    const size_t H = 1 + 4 + 1 + 4; // magic + body_len + endian + hdr_crc
    while (true) {
      if (conn->rx->readable_bytes() < H) break;
      uint8_t hdrbuf[10];
      size_t got = conn->rx->peek_bytes(hdrbuf, H);
      if (got < H) break;

      PacketHeader hdr;
      hdr.magic = hdrbuf[0];
      uint32_t bl; memcpy(&bl, hdrbuf + 1, 4);
      hdr.endian = hdrbuf[5];
      memcpy(&hdr.hdr_crc, hdrbuf + 6, 4);
      uint32_t crc = crc32_calc(hdrbuf, 6);
      if (hdr.magic != 0x7e || hdr.hdr_crc != crc) {
        g_metrics.parse_errors++;
        throw_close(conn);
        return;
      }
      hdr.body_len = hdr.endian ? bswap32_u32(bl) : bl;

      const uint32_t MAX_BODY = 16 * 1024 * 1024; // 16MB 防御
      if (hdr.body_len > MAX_BODY) {
        g_metrics.parse_errors++;
        throw_close(conn);
        return;
      }
      if (conn->rx->readable_bytes() < H + hdr.body_len) break;

      conn->rx->consume(H);
      std::vector<RingBuffer::StealEntry> stolen;
      if (!conn->rx->steal_body_after(0, hdr.body_len, stolen)) {
        g_metrics.drops++;
        break;
      }
      g_metrics.rx_pkts++;

      WorkerPool::Job job = [fd = conn->fd, stolen = std::move(stolen), this]() mutable {
        business_worker_echo(fd, stolen, taskq_);
      };
      workers_.submit(job);
    }
  }

  void on_writable(std::unique_ptr<Connection> &conn) {
    if (conn->out_empty()) return;
    iovec iovs[OUT_RING_CAP];
    std::vector<size_t> idxs; idxs.reserve(OUT_RING_CAP);
    size_t cnt = conn->out_peek(iovs, OUT_RING_CAP, idxs);
    if (cnt == 0) return;

    ssize_t n = writev(conn->fd, iovs, (int)cnt);
    if (n > 0) {
      g_metrics.tx_bytes += (uint64_t)n;
      g_metrics.tx_pkts++;
      conn->out_advance((size_t)n, pool_);
    } else {
      if (errno != EAGAIN && errno != EWOULDBLOCK) {
        throw_close(conn);
      }
    }
  }

  void on_timeout(int fd, std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    auto it = conns.find(fd);
    if (it == conns.end()) return;
    if (now_ms() - it->second->last_active_ms > idle_ms_) {
      g_metrics.timeouts++;
      close_conn(it->second);
    } else {
      wheel_.add(fd, now_ms() + active_ms_); // 延后检查
    }
  }

  void process_incoming_tasks(std::unordered_map<int, std::unique_ptr<Connection>> &conns) {
    ResponseTask t;
    while (taskq_.dequeue(t)) {
      auto it = conns.find(t.fd);
      if (it == conns.end()) {
        for (auto &e : t.entries) pool_.release_ref(e.blk);
        continue;
      }
      auto &conn = it->second;
      bool ok_all = true;
      for (auto &e : t.entries) {
        if (!conn->out_push(e)) { ok_all = false; break; }
      }
      if (!ok_all) {
        for (auto &e : t.entries) pool_.release_ref(e.blk);
        g_metrics.drops++;
      } else {
        epoll_event ev{}; ev.events = EPOLLIN | EPOLLOUT | EPOLLET; ev.data.fd = conn->fd;
        epoll_ctl(epfd_, EPOLL_CTL_MOD, conn->fd, &ev); // 允许失败
      }
    }
  }

  void serve_metrics(int mfd) {
    for (;;) {
      sockaddr_in cli; socklen_t len = sizeof cli;
      int c = accept4(mfd, (sockaddr *)&cli, &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
      if (c < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        else break;
      }
      char buf[1024]; int n = (int)recv(c, buf, sizeof(buf) - 1, 0);
      if (n <= 0) { close(c); continue; }
      buf[n] = 0;
      if (strncmp(buf, "GET /metrics", 12) == 0) {
        std::string body = metrics_text();
        char hdr[256];
        int hn = snprintf(hdr, sizeof hdr,
                          "HTTP/1.1 200 OK\r\n"
                          "Content-Length: %zu\r\n"
                          "Content-Type: text/plain; version=0.0.4\r\n"
                          "Connection: close\r\n"
                          "\r\n",
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

  void close_conn(std::unique_ptr<Connection> &conn) {
    if (!conn) return;
    g_metrics.closed++;
    cpool_.release(conn);
  }
  void throw_close(std::unique_ptr<Connection> &conn) { close_conn(conn); }
};

// -------------------------- 全局 & main --------------------------
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
  size_t total_blocks = (size_t)ncpu * DEFAULT_BLOCKS_PER_CPU;
  LOG_INFO("starting server on port %u with %d CPUs, total_blocks=%zu", port, ncpu, total_blocks);

  BufferPool pool(total_blocks);
  size_t max_conn = 1000000;
  ConnectionPool cpool(max_conn, pool);
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

  std::vector<std::thread> reactor_threads;
  std::vector<std::unique_ptr<Reactor>> reactors;
  for (int i = 0; i < ncpu; ++i) {
    reactors.emplace_back(new Reactor(i, listen_fd, pool, cpool, workers, DEFAULT_IDLE_MS, DEFAULT_ACTIVE_MS));
    reactors.back()->set_metrics_port(metrics_port);
    reactor_threads.emplace_back([&r = reactors.back()]() { r->run(); });
  }

  std::thread metrics_printer([&]{
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
        "[metrics] acc=%llu cls=%llu rx=%llu tx=%llu rx/s=%.0fB tx/s=%.0fB pkts(rx=%llu tx=%llu) drop=%llu err=%llu to=%llu\n",
        (unsigned long long)g_metrics.accepted.load(),
        (unsigned long long)g_metrics.closed.load(),
        (unsigned long long)rx,
        (unsigned long long)tx,
        rxrate, txrate,
        (unsigned long long)g_metrics.rx_pkts.load(),
        (unsigned long long)g_metrics.tx_pkts.load(),
        (unsigned long long)g_metrics.drops.load(),
        (unsigned long long)g_metrics.parse_errors.load(),
        (unsigned long long)g_metrics.timeouts.load()
      );
    }
  });

  while (!g_terminate.load()) std::this_thread::sleep_for(std::chrono::milliseconds(200));

  LOG_INFO("shutdown requested, stopping reactors");
  for (auto &r : reactors) r->stop();
  for (auto &t : reactor_threads) if (t.joinable()) t.join();
  metrics_printer.join();
  LOG_INFO("server stopped");
  return 0;
}
