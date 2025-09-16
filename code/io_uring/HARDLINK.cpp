#include <liburing.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <fcntl.h>

#define READ_USERDATA  2001
#define WRITE_USERDATA 2002

int main() {
    io_uring ring;
    io_uring_queue_init(256, &ring, 0);

    // 建立监听 socket
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    int yes = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(8080);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    bind(listen_fd, (sockaddr*)&addr, sizeof(addr));
    listen(listen_fd, 128);

    std::cout << "listening on 8080...\n";

    // 接收一个连接
    sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    int conn_fd = accept(listen_fd, (sockaddr*)&client_addr, &client_len);
    std::cout << "client fd=" << conn_fd << "\n";

    char buf[1024];

    // 1. read SQE
    io_uring_sqe* sqe1 = io_uring_get_sqe(&ring);
    io_uring_prep_read(sqe1, conn_fd, buf, sizeof(buf), 0);
    sqe1->flags |= IOSQE_IO_HARDLINK;   // ⚠️ 与 IO_LINK 不同
    sqe1->user_data = READ_USERDATA;

    // 2. write SQE (会执行，即使 read 失败)
    io_uring_sqe* sqe2 = io_uring_get_sqe(&ring);
    io_uring_prep_write(sqe2, conn_fd, buf, sizeof(buf), 0);
    sqe2->user_data = WRITE_USERDATA;

    // 提交
    io_uring_submit(&ring);

    // 等待完成
    for (int i = 0; i < 2; i++) {
        io_uring_cqe* cqe;
        int ret = io_uring_wait_cqe(&ring, &cqe);
        if (ret < 0) break;

        if (cqe->user_data == READ_USERDATA) {
            if (cqe->res < 0) {
                std::cerr << "read failed: " << strerror(-cqe->res) << "\n";
            } else {
                std::cout << "read bytes=" << cqe->res << "\n";
            }
        } else if (cqe->user_data == WRITE_USERDATA) {
            if (cqe->res < 0) {
                std::cerr << "write failed: " << strerror(-cqe->res) << "\n";
            } else {
                std::cout << "write bytes=" << cqe->res << "\n";
            }
        }

        io_uring_cqe_seen(&ring, cqe);
    }

    close(conn_fd);
    close(listen_fd);
    io_uring_queue_exit(&ring);
}
