// 时间轮定时器，基于环形槽和无锁队列实现。
// 用于高性能场景下的定时任务（如连接超时管理）。

class TimingWheel {
public:
    // 定时器条目：fd + 过期时间戳
    struct Entry {
        int fd;
        uint64_t expire_ms;
    };

    // tick_ms: 每个时间槽的粒度（毫秒）
    // slots: 槽数，会被规范化为 2 的幂
    explicit TimingWheel(uint64_t tick_ms, size_t slots)
        : tick_ms_(tick_ms), 
          slots_(normalize_pow2(slots)), 
          slot_mask_(slots_ - 1),
          initialized_(false), 
          last_slot_index_(0) 
    {
        // 初始化槽，每个槽是一个无锁环形队列
        slots_q_.reserve(slots_);
        for (size_t i = 0; i < slots_; ++i)
            slots_q_.emplace_back(std::make_unique<MPMCRing<Entry>>(1 << 14));
    }

    // 添加一个定时器条目
    inline void add(int fd, uint64_t expire_ms) {
        Entry e{fd, expire_ms};
        size_t idx = slot_index(expire_ms);
        // 最多尝试 64 次入队，失败则丢弃
        for (int i = 0; i < 64; ++i) {
            if (slots_q_[idx]->enqueue(e))
                return;
            std::this_thread::yield(); // 让出 CPU，避免忙等
        }
        g_metrics.drops.fetch_add(1, std::memory_order_relaxed);
    }

    // 推进时间轮
    // now_ms: 当前时间戳
    // on_timeout: 回调函数，触发超时时执行
    template<typename F>
    void tick(uint64_t now_ms, F on_timeout) {
        // 首次初始化，设置当前位置
        if (!initialized_.load(std::memory_order_acquire)) {
            size_t idx = (now_ms / tick_ms_) & slot_mask_;
            last_slot_index_.store(idx, std::memory_order_release);
            initialized_.store(true, std::memory_order_release);
            return;
        }

        // 计算目标槽位置
        size_t target = (now_ms / tick_ms_) & slot_mask_;
        size_t cur = last_slot_index_.load(std::memory_order_relaxed);

        // 逐槽推进，处理到目标槽为止,避免遗漏
        while (cur != target) {
            drain(cur, now_ms, on_timeout);
            cur = (cur + 1) & slot_mask_;
        }
        last_slot_index_.store(cur, std::memory_order_relaxed);

        // 当前槽轻量级检查，避免 backlog 堆积, 分散开销
        drain_light(cur, now_ms, on_timeout);
    }

private:
    // 完全 drain 某个槽（清空队列）
    template<typename F>
    void drain(size_t idx, uint64_t now_ms, F &on_timeout) {
        Entry e;
        while (slots_q_[idx]->dequeue(e)) {
            if (e.expire_ms <= now_ms) {
                // 到期 -> 触发回调
                on_timeout(e.fd);
            } else {
                // 未到期 -> 重新放回时间轮
                size_t target_idx = slot_index(e.expire_ms);
                // 如果计算出的槽和当前槽相同，说明 expire_ms 太接近当前 tick
                // 向后推一个 tick，避免立即被再次扫描
                if (target_idx == idx) {
                    e.expire_ms += tick_ms_;
                }
                add(e.fd, e.expire_ms);
            }
        }
    }

    // 轻量级 drain：只尝试拉取固定数量条目，避免长时间阻塞
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

    // 将槽数规范化为 2 的幂（方便按位与取模）
    static size_t normalize_pow2(size_t x) {
        size_t p = 1;
        while (p < x)
            p <<= 1;
        return p;
    }

    // 根据过期时间计算对应槽索引
    inline size_t slot_index(uint64_t expire_ms) const {
        return ((expire_ms / tick_ms_) & slot_mask_);
    }

    const uint64_t tick_ms_;   // 每个槽代表的时间跨度
    const size_t slots_;       // 槽总数（2 的幂）
    const size_t slot_mask_;   // = slots - 1，用于快速取模
    std::vector<std::unique_ptr<MPMCRing<Entry>>> slots_q_; // 槽队列
    std::atomic<bool> initialized_;    // 是否初始化过
    std::atomic<size_t> last_slot_index_; // 上一次 tick 的槽索引
};

/*
对比 drain 和 drain_light

drain(idx, now_ms, on_timeout)
会把该槽里的所有元素都 dequeue 出来，直到为空。
→ 适合在“补偿性清理”场景，比如：tick 跳过了几个槽，需要把中间的所有槽彻底清理。

drain_light(idx, now_ms, on_timeout)
只尝试取出 最多 64 个元素，并不会清空整个槽。
→ 适合在“当前时间槽”的快速扫描场景。即：只轻量地处理部分超时的条目，避免一次性拖垮主线程。

为什么需要 drain_light

性能保护
如果某个槽里堆积了大量定时器事件（例如成千上万），而每次 tick 都要完全 drain 掉，可能导致 tick 执行时间过长，阻塞 IO 线程。
drain_light 通过限制 64 次 dequeue，可以把耗时均摊到多次 tick。

降低延迟抖动
在 IO/定时器混合场景下，如果 drain 一次处理过多，会让当前时间片的延迟上升。drain_light 控制上限，可以保证每次 tick 都是“小步快跑”。

当前槽的特殊性

tick() 中循环推进时，历史槽（已经过期的槽）必须完全 drain —— 所以用 drain。

但对 当前正在走的槽，只需要 opportunistic（机会性）地清理一小部分即可 —— 所以用 drain_light。

总结一句

drain_light 适合 高频 tick、事件量大 的场景，用来限制单次 tick 的处理开销；
drain 用在 补偿清理（跨过的槽必须全部清理），确保过期事件不会被遗漏。
*/