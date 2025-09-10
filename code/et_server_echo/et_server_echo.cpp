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
#include <vector>
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

// 简单环形缓冲区
struct RingBuffer {
    std::vector<char> buf;
    size_t head = 0, tail = 0;

    RingBuffer(size_t size) : buf(size) {}

    size_t size() const { return (tail + buf.size() - head) % buf.size(); }
    size_t free_space() const { return buf.size() - size() - 1; }

    size_t write(const char* data, size_t n) {
        size_t written = 0;
        while (n > 0 && free_space() > 0) {
            size_t pos = tail % buf.size();
            size_t chunk = std::min(n, buf.size() - pos);
            chunk = std::min(chunk, free_space());
            std::memcpy(buf.data() + pos, data + written, chunk);
            tail = (tail + chunk) % buf.size();
            n -= chunk;
            written += chunk;
        }
        return written;
    }

    size_t read(char* data, size_t n) {
        size_t read_bytes = 0;
        while (n > 0 && size() > 0) {
            size_t pos = head % buf.size();
            size_t chunk = std::min(n, buf.size() - pos);
            chunk = std::min(chunk, size());
            std::memcpy(data + read_bytes, buf.data() + pos, chunk);
            head = (head + chunk) % buf.size();
            n -= chunk;
            read_bytes += chunk;
        }
        return read_bytes;
    }
};

struct Connection {
    int fd;
    RingBuffer buffer;
    Connection(int fd_) : fd(fd_), buffer(BUF_SIZE) {}
};

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
    ev.events = EPOLLIN | EPOLLET;
    ev.data.ptr = nullptr; // listen_fd 标记为 nullptr
    epoll_ctl(epfd, EPOLL_CTL_ADD, listen_fd, &ev);

    std::vector<Connection*> connections;

    while (true) {
        int nfds = epoll_wait(epfd, events, MAX_EVENTS, -1);
        if (nfds < 0) {
            if (errno == EINTR) continue;
            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.ptr == nullptr) {
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

                    Connection* conn = new Connection(client_fd);
                    connections.push_back(conn);

                    epoll_event cev{};
                    cev.events = EPOLLIN | EPOLLET;
                    cev.data.ptr = conn;
                    epoll_ctl(epfd, EPOLL_CTL_ADD, client_fd, &cev);
                }
            } else {
                Connection* conn = (Connection*)events[i].data.ptr;
                char buf[BUF_SIZE];
                while (true) {
                    ssize_t n = recv(conn->fd, buf, sizeof(buf), 0);
                    if (n > 0) {
                        // 收到就回显
                        size_t sent = 0;
                        while (sent < (size_t)n) {
                            ssize_t m = send(conn->fd, buf + sent, n - sent, 0);
                            if (m <= 0) {
                                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                                goto close_conn;
                            }
                            sent += m;
                        }
                    } else if (n == 0) {
                        // 关闭
                        goto close_conn;
                    } else {
                        if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                        goto close_conn;
                    }
                }
                continue;

            close_conn:
                epoll_ctl(epfd, EPOLL_CTL_DEL, conn->fd, nullptr);
                close(conn->fd);
                delete conn;
            }
        }
    }

    close(listen_fd);
    close(epfd);
    return 0;
}

