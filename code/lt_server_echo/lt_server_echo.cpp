#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <atomic>

constexpr int PORT = 9000;
constexpr int MAX_EVENTS = 1024;
constexpr int BUF_SIZE = 8192;

static std::atomic<int> connected_num(0);

// 设置 socket 非阻塞
int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1) return -1;
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main() {
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    setsockopt(listen_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(listen_fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }

    if (listen(listen_fd, SOMAXCONN) < 0) {
        perror("listen");
        return 1;
    }

    set_nonblocking(listen_fd);

    int epfd = epoll_create1(0);
    epoll_event ev{}, events[MAX_EVENTS];
    ev.events = EPOLLIN; // LT 模式默认
    ev.data.fd = listen_fd;
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    while (true) {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            int fd = events[i].data.fd;
            if (fd == listen_fd) {
                // 新连接
                while (true) {
                    sockaddr_in cli_addr{};
                    socklen_t cli_len = sizeof(cli_addr);
                    int client_fd = accept(listen_fd, (sockaddr*)&cli_addr, &cli_len);
                    if (client_fd < 0) {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        perror("accept");
                        continue;
                    }
                    set_nonblocking(client_fd);
                    setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

                    connected_num++;
                    std::cout << "connected_num = " << connected_num << std::endl;

                    epoll_event cev{};
                    cev.events = EPOLLIN; // LT 模式
                    cev.data.fd = client_fd;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev);
                }
            } else {
                // 数据就绪，LT 会持续触发
                char buf[BUF_SIZE];
                while (true) {
                    ssize_t n = recv(fd, buf, sizeof(buf), 0);
                    if (n > 0) {
                        size_t sent = 0;
                        while (sent < (size_t)n) {
                            ssize_t m = send(fd, buf + sent, n - sent, 0);
                            if (m <= 0) {
                                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                                goto close_conn;
                            }
                            sent += m;
                        }
                    } else if (n == 0) {
                        // 客户端关闭
                        goto close_conn;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        goto close_conn;
                    }
                }
                continue;

            close_conn:
                epoll_ctl(epfd, EPOLL_CTL_DEL, fd, nullptr);
                close(fd);
            }
        }
    }

    close(listen_fd);
    close(epfd);
    return 0;
}
