// connect_test.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstring>

#define WIN32_LEAN_AND_MEAN
#include <winsock2.h>
#include <ws2tcpip.h>
#include <ws2def.h>

void print_last_error(const char* msg) {
    std::cerr << msg << ": " << WSAGetLastError() << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc < 5) {
        std::cerr << "Usage: " << argv[0]
            << " <local_ip> <server_ip> <server_port> <conn_count>\n";
        return 1;
    }

    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed" << std::endl;
        return 1;
    }

    const char* local_ip = argv[1];
    const char* server_ip = argv[2];
    int server_port = std::atoi(argv[3]);
    int conn_count = std::atoi(argv[4]);

    std::vector<SOCKET> sockets;
    sockets.reserve(conn_count);

    for (int i = 0; i < conn_count; ++i) {
        SOCKET fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd == INVALID_SOCKET) {
            print_last_error("socket");
            break;
        }

        // 设置 SO_REUSEADDR 避免端口占用问题
        int opt = 1;
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, (char*)&opt, sizeof(opt));

        int reuse_unicast = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_PORT_SCALABILITY, (char*)&reuse_unicast, sizeof(reuse_unicast)) != 0) {
            int err = WSAGetLastError();
            std::cerr << "setsockopt SO_REUSE_UNICASTPORT failed: " << err << std::endl;
            // 注意：如果系统不支持该选项，此调用可能失败，这在你的 Windows 11 上不太可能发生。
        }

        // 绑定到指定 local_ip + 动态端口
        sockaddr_in local_addr{};
        local_addr.sin_family = AF_INET;
        local_addr.sin_port = htons(0);
        inet_pton(AF_INET, local_ip, &local_addr.sin_addr);

        if (bind(fd, (sockaddr*)&local_addr, sizeof(local_addr)) == SOCKET_ERROR) {
            int err = WSAGetLastError();
            std::cout << "bind returned SOCKET_ERROR, WSAGetLastError=" << err << std::endl;
            closesocket(fd);
            break;
        }

        // 连接服务端
        sockaddr_in server_addr{};
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(server_port);
        inet_pton(AF_INET, server_ip, &server_addr.sin_addr);

        if (connect(fd, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
            print_last_error("connect");
            closesocket(fd);
            break;
        }

        sockets.push_back(fd);

        if ((i + 1) % 1000 == 0) {
            std::cout << "Established " << (i + 1) << " connections\n";
        }
    }

    std::cout << "Total connections: " << sockets.size() << std::endl;

    // 挂起，保持连接。按回车键退出
    std::cout << "Press Enter to exit..." << std::endl;
    std::cin.get();   // 等待用户按键

    // 退出前关闭所有连接
    for (SOCKET fd : sockets) {
        closesocket(fd);
    }

    WSACleanup();
    return 0;
}

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门使用技巧: 
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5. 转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
