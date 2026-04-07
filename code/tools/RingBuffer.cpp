/*RingBuffer 并不是传统意义上的「固定大小环形缓冲区」，而是基于 链表块（BufferBlock）+ BufferPool 
实现的“无界环”，核心特点：
1、内部用多个 固定大小的内存块（BUFFER_BLOCK_SIZE） 串起来。
2、读写通过 (head_, head_off_) 和 (tail_, tail_off_) 两个指针和偏移量来管理。
3、内存块来自 BufferPool，用完会归还，避免频繁 malloc/free。
支持：
1、普通读写（writable/produce/peek/consume）。
2、零拷贝“偷取 body”（steal_body_after）。
*/
class RingBuffer {
public:
  // 构造：需要传入 BufferPool（外部池子分配内存块）
  RingBuffer(BufferPool &pool)
      : pool_(pool), head_(nullptr), tail_(nullptr),
        head_off_(0), tail_off_(0) {
    append_block(); // 初始化时至少放入一个 block
  }

  // 析构：释放所有挂在链表上的 block 回到池子
  ~RingBuffer() {
    BufferBlock *cur = head_;
    while (cur) {
      BufferBlock *nxt = cur->next;
      pool_.release_ref(cur); // 池子里用引用计数回收
      cur = nxt;
    }
    head_ = tail_ = nullptr;
  }

  // 获取可写区域（保证不跨块），返回 iovec
  iovec writable_region() {
    ensure_tail(); // 确保 tail 指向可用的 block
    return iovec{
      tail_->data + tail_off_,               // 可写指针
      BUFFER_BLOCK_SIZE - tail_off_ - 1      // 剩余容量
    };
  }

  // 生产者写入 n 字节后调用，前移 tail_off_
  void produce(size_t n) {
    tail_off_ += n;
    if (tail_off_ >= BUFFER_BLOCK_SIZE - 1) {
      // 当前块写满了，挂接新块
      append_block();
      tail_off_ = 0;
    }
  }

  // 从 head 开始复制（peek）最多 n 字节，不消费
  size_t peek_bytes(uint8_t *dst, size_t n) {
    if (!head_)
      return 0;
    size_t copied = 0;
    BufferBlock *cur = head_;
    size_t off = head_off_;
    size_t remain = n;

    while (remain > 0 && cur) {
      size_t avail = (cur == tail_)
          ? (tail_off_ - off)              // head == tail：最后一个块
          : (BUFFER_BLOCK_SIZE - off);     // 中间块可读到结尾

      if (avail == 0) break;

      size_t take = std::min(avail, remain);
      memcpy(dst + copied, cur->data + off, take);

      copied += take;
      remain -= take;
      off += take;

      if (off >= BUFFER_BLOCK_SIZE) { // 跨块
        cur = cur->next;
        off = 0;
      }
    }
    return copied;
  }

  // 消费 n 字节（移动 head_off_，必要时释放 block）
  void consume(size_t n) {
    size_t remain = n;
    while (remain > 0 && head_) {
      size_t avail = (head_ == tail_)
          ? (tail_off_ - head_off_)        // 最后一个块只读到 tail_off_
          : (BUFFER_BLOCK_SIZE - head_off_);

      if (avail > remain) {
        head_off_ += remain;
        return;
      }

      remain -= avail;

      // 当前 block 完全消费，释放
      BufferBlock *old = head_;
      head_ = old->next;
      pool_.release_ref(old);
      head_off_ = 0;

      if (!head_) { // 如果消耗到空，重置 tail
        tail_ = nullptr;
        tail_off_ = 0;
        break;
      }
    }
  }

  // 统计总的可读字节数（可能跨多个 block）
  size_t readable_bytes() const {
    if (!head_) return 0;
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

  // 零拷贝偷取 body（常用于协议：header 已读，body 想直接引用）
  struct StealEntry {
    BufferBlock *blk; // 指向的块
    size_t offset;    // 起始偏移
    size_t len;       // 长度
  };
  bool steal_body_after(size_t header_len, size_t body_len,
                        std::vector<StealEntry> &out) {
    // 不够数据，直接失败
    if (readable_bytes() < header_len + body_len)
      return false;

    // 跳过 header
    BufferBlock *cur = head_;
    size_t off = head_off_;
    size_t skip = header_len;
    while (skip > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off)
                                    : (BUFFER_BLOCK_SIZE - off);
      if (avail > skip) {
        off += skip;
        skip = 0;
        break;
      }
      skip -= avail;
      cur = cur->next;
      off = 0;
    }

    // 收集 body
    size_t remain = body_len;
    std::vector<BufferBlock *> to_retain;
    while (remain > 0 && cur) {
      size_t avail = (cur == tail_) ? (tail_off_ - off)
                                    : (BUFFER_BLOCK_SIZE - off);
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

    // 增加 block 的引用计数（避免被 consume 释放）
    for (BufferBlock *b : to_retain) pool_.retain(b);

    // 前移 ring，释放 header+body
    consume(header_len + body_len);
    return true;
  }

private:
  BufferPool &pool_;       // 内存池
  BufferBlock *head_;      // 当前读块
  BufferBlock *tail_;      // 当前写块
  size_t head_off_, tail_off_; // 读写偏移

  // 分配新块并挂到 tail
  void append_block() {
    BufferBlock *b = pool_.acquire();
    if (!b) die("BufferPool exhausted");
    b->next = nullptr;
    if (!head_) {
      head_ = tail_ = b;
      head_off_ = tail_off_ = 0;
    } else {
      tail_->next = b;
      tail_ = b;
    }
  }

  // 确保 tail 可写，不够就追加 block
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
