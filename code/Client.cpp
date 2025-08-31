#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <chrono>
#pragma comment(lib, "ws2_32.lib")  // 链接 Winsock 库

using namespace std::chrono_literals;

// 协议头
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  magic;
    uint32_t body_len;
    uint8_t  endian;
    uint32_t hdr_crc;
};
#pragma pack(pop)

// CRC32 计算
static inline uint32_t crc32_calc(const void *data, size_t len) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j)
                c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : (c >> 1);
            table[i] = c;
        }
        init = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    const uint8_t *p = reinterpret_cast<const uint8_t *>(data);
    for (size_t i = 0; i < len; ++i)
        c = table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

int seq = 0;

int main() {
    const char* server_ip = "192.168.169.130";
    const int server_port = 9000;

    // 初始化 Winsock
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed: " << WSAGetLastError() << "\n";
        return 1;
    }

    std::thread* works[8];
    for(int i = 0; i < 1; i++)
    { 
        works[i] = new std::thread([&] {
            int loop_cnt = 2;
            do {
                // 创建 socket
                SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
                if (sock == INVALID_SOCKET) {
                    std::cerr << "socket failed: " << WSAGetLastError() << "\n";
                    WSACleanup();
                    return 1;
                }

                // 设置服务器地址
                sockaddr_in server_addr{};
                server_addr.sin_family = AF_INET;
                server_addr.sin_port = htons(server_port);
                if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
                    std::cerr << "inet_pton failed: " << WSAGetLastError() << "\n";
                    closesocket(sock);
                    WSACleanup();
                    return 1;
                }

                // 连接服务器
                if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
                    std::cerr << "connect failed: " << WSAGetLastError() << "\n";
                    closesocket(sock);
                    WSACleanup();
                    return 1;
                }
                std::cout << "Connected to server " << server_ip << ":" << server_port << "\n";

                std::string body = std::to_string(++seq);

                // 构造协议头
                PacketHeader hdr{};
                hdr.magic = 0x7e;  // 魔数
                hdr.body_len = static_cast<uint32_t>(body.size());
                hdr.endian = 0;     // 小端
                hdr.hdr_crc = 0;

                // 计算 CRC（不包含 hdr_crc 本身）
                hdr.hdr_crc = crc32_calc(&hdr, sizeof(hdr) - sizeof(hdr.hdr_crc));

                std::vector<uint8_t> packet(sizeof(hdr) + body.size());
                memcpy(packet.data(), &hdr, sizeof(hdr));
                memcpy(packet.data() + sizeof(hdr), body.data(), body.size());

                int sent = send(sock, reinterpret_cast<const char*>(packet.data()), (int)packet.size(), 0);
                if (sent == SOCKET_ERROR) {
                    std::cerr << "send failed: " << WSAGetLastError() << "\n";
                    closesocket(sock);
                    WSACleanup();
                    return 1;
                }
                std::cout << "Sent " << sent << " bytes: " << seq << "\n";

                // 接收数据
                char buf[1024];
                int n = recv(sock, buf, sizeof(buf) - 1, 0);
                if (n == SOCKET_ERROR) {
                    std::cerr << "recv failed: " << WSAGetLastError() << "\n";
                }
                else if (n > 0) {
                    buf[n] = '\0';
                    std::cout << "Received " << n << " bytes: " << buf << "\n";
                }

                // 关闭连接
                closesocket(sock);
            } while (--loop_cnt > 0);
        });
    }

    for (int i = 0; i < 1; i++)
    {
        if(works[i]->joinable())
        works[i]->join();
    }

    while (true)
    {
        std::this_thread::sleep_for(1s);
    }

    // 清理 Winsock
    WSACleanup();
    return 0;
}
