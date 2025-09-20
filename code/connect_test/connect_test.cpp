#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <vector>

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <local_ip> <server_ip> <server_port> <conn_count>\n";
        return 1;
    }

    const char* local_ip  = argv[1];
    const char* server_ip = argv[2];
    int server_port       = std::atoi(argv[3]);
    int conn_count        = std::atoi(argv[4]);

    std::vector<int> sockets;
    sockets.reserve(conn_count);

    for (int i = 0; i < conn_count; ++i) {
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            perror("socket");
            break;
        }

        // 设置 SO_REUSEADDR 避免端口占用问题
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        // 绑定到指定 local_ip + 动态端口
        sockaddr_in local_addr{};
        local_addr.sin_family = AF_INET;
        local_addr.sin_port   = htons(0); // 让内核分配可用端口
        inet_pton(AF_INET, local_ip, &local_addr.sin_addr);

        if (bind(fd, (sockaddr*)&local_addr, sizeof(local_addr)) < 0) {
            perror("bind");
            close(fd);
            break;
        }

        // 连接服务端
        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port   = htons(server_port);
        inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

        if (connect(fd, (sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            perror("connect");
            close(fd);
            break;
        }

        sockets.push_back(fd);

        if ((i + 1) % 1000 == 0) {
            std::cout << "Established " << (i + 1) << " connections\n";
        }
    }

    std::cout << "Total connections: " << sockets.size() << std::endl;

    // 挂起，保持连接
    pause();

    // 退出前关闭所有连接
    for (int fd : sockets) {
        close(fd);
    }

    return 0;
}
