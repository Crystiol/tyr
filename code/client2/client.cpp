#include <iostream>
#include <cstring>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <vector>

// 协议头
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  magic;
    uint32_t body_len;
    uint8_t  endian;
    uint32_t hdr_crc;
};
#pragma pack(pop)

static inline uint32_t crc32_calc(const void *data, size_t len) {
    static uint32_t table[256];
    static bool init = false;
    if (!init) {
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int j = 0; j < 8; ++j)
                c = c & 1 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        init = true;
    }
    uint32_t c = 0xFFFFFFFFu;
    const uint8_t *p = (const uint8_t *)data;
    for (size_t i = 0; i < len; ++i)
        c = table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;
}

int seq = 1;

int main() {
    const char* server_ip = "127.0.0.1";
    const int server_port = 9000;

    while(1){
        // 创建 socket
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            perror("socket");
            return 1;
        }

        // 设置服务器地址
        struct sockaddr_in server_addr;
        memset(&server_addr, 0, sizeof(server_addr));
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(server_port);
        if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
            perror("inet_pton");
            close(sock);
            return 1;
        }

        // 连接服务器
        if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            perror("connect");
            close(sock);
            return 1;
        }
        std::cout << "Connected to server " << server_ip << ":" << server_port << "\n";

        std::string body = std::to_string(seq++);

        // 构造协议头
        PacketHeader hdr{};
        hdr.magic    = 0x7e;  // 魔数
        hdr.body_len = static_cast<uint32_t>(body.size());
        hdr.endian   = 0;     // 0=小端
        hdr.hdr_crc  = 0;

        // 计算CRC（不包含hdr_crc本身）
        hdr.hdr_crc = crc32_calc(&hdr, sizeof(hdr) - sizeof(hdr.hdr_crc));

        std::vector<uint8_t> packet(sizeof(hdr) + body.size());
        memcpy(packet.data(), &hdr, sizeof(hdr));
        memcpy(packet.data() + sizeof(hdr), body.data(), body.size());

        ssize_t sent = send(sock, packet.data(), packet.size(), 0);
        if (sent < 0) {
            perror("send");
            close(sock);
            return 1;
        }
        std::cout << "Sent " << sent << " bytes: " << seq << "\n";

        // 接收数据
        char buf[1024];
        ssize_t n = read(sock, buf, sizeof(buf) - 1);
        if (n < 0) {
            perror("read");
        } else {
            buf[n] = '\0';
            std::cout << "Received " << n << " bytes: " << buf << "\n";
        }

        // 关闭连接
        close(sock);
    }

    return 0;
}
