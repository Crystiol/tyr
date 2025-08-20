版本V1
用 io_uring_prep_accept / io_uring_prep_recv / io_uring_prep_send 完成异步 accept/read/write。
提交 SQE 后等待 CQE。每个 CQE 的 user_data 存放一个 IoRequest*，用于关联上下文。

BufferPool：小块优先，避免小包频繁占用 4KB；大包回退，保持吞吐。Block 带引用计数，send 完成后
释放。

WorkerPool：示例业务直接把接收到的 buffer 发回（echo），演示零拷贝回写（worker 直接提交 send，
send 完成释放 buffer）。在更复杂协议下，worker 可以解析 header 决定是否合并/拆分/转发等。

Accept 保持连续提交（初始提交多个 accept），确保高并发连接到来时 SQ 中有足够的 accept 请求，
减少延迟。

错误处理和连接管理写得相对简洁：真实使用应当增强（例如：对大量连接判断 backpressure、限速、
windowing、send 排队与流控等）。

若想进一步增强性能：可以使用 io_uring_register_buffers（注册固定缓冲区数组，减少内核/用户复制）
、使用 MSG_ZEROCOPY（对于大批发包）、用专门的 per-core ring（io_uring supports SQPOLL/IOPOLL）
，以及更细的 NUMA/CPU 绑定。

版本V2
增加更完整的协议解析（header+body，类似之前 epoll 版本的 steal/zero-copy），并演示如何在 
worker 中安全引用多个 BufferBlock（并释放）。

改造为注册缓冲区版本（io_uring_register_buffers）以进一步减少复制，并把读写改为使用 fixed 
buffers（需要考虑顺序/并发）。

把 send/recv 改为使用 sendmsg/recvmsg + iovec，支持 scatter/gather 更高效的批量 send。

版本V3