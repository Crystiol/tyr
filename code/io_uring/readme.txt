可使用SQPOLL进行优化
struct io_uring_params params;
memset(&params, 0, sizeof(params));
params.flags = IORING_SETUP_SQPOLL;
params.sq_thread_cpu = cpu_id_;
params.sq_thread_idle = 2000; // ms
if (io_uring_queue_init_params(4096, &ring_, &params) < 0) {
	// fallback - try without SQPOLL
	memset(&params, 0, sizeof(params));
	if (io_uring_queue_init_params(4096, &ring_, &params) < 0) {
		die("io_uring_queue_init_params");
	} else {
		logger_->debug("io_uring initialized without SQPOLL");
	}
} else {
	logger_->debug("io_uring initialized WITH SQPOLL");
}

while (!global_stop_.load()) {
	process_incoming_tasks_and_submit_writes(conns);

	// 批量提交当前 SQE（如果使用 SQPOLL，尽管可以不用submit，但这一步对提交线程仍是有益的）
	// 这里可以起到唤醒内核线程的作用
	int ret = io_uring_submit(&ring_);
	if (ret < 0 && errno != EINTR) {
		// log but continue
		logger_->error("io_uring_submit ret=%d errno=%d", ret, errno);
	}
	...
}

