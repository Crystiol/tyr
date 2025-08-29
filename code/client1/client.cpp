#include <boost/asio.hpp>
#include <iostream>
#include <vector>
#include <string>
#include <cstdint>
#include <zlib.h>   // 用于CRC32校验

using boost::asio::ip::tcp;

// 协议头
#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  magic;
    uint32_t body_len;
    uint8_t  endian;
    uint32_t hdr_crc;
};
#pragma pack(pop)

// 简单的CRC32计算
uint32_t crc32_calc(const void* data, size_t len) {
    return ::crc32(0L, reinterpret_cast<const unsigned char*>(data), len);
}

int main() {
    try {
        boost::asio::io_context io;

        // 改成你的服务器地址和端口
        tcp::resolver resolver(io);
        auto endpoints = resolver.resolve("127.0.0.1", "8080");
        tcp::socket socket(io);
        boost::asio::connect(socket, endpoints);

        std::cout << "Connected to server.\n";

        while (true) {
            std::string body;
            std::cout << "Enter message (or 'quit'): ";
            std::getline(std::cin, body);

            if (body == "quit" || !std::cin) {
                break;
            }

            // 构造协议头
            PacketHeader hdr{};
            hdr.magic    = 0x42;  // 魔数
            hdr.body_len = static_cast<uint32_t>(body.size());
            hdr.endian   = 0;     // 0=小端
            hdr.hdr_crc  = 0;

            // 计算CRC（不包含hdr_crc本身）
            hdr.hdr_crc = crc32_calc(&hdr, sizeof(hdr) - sizeof(hdr.hdr_crc));

            // 打包并发送
            std::vector<boost::asio::const_buffer> bufs;
            bufs.push_back(boost::asio::buffer(&hdr, sizeof(hdr)));
            if (!body.empty())
                bufs.push_back(boost::asio::buffer(body));
            boost::asio::write(socket, bufs);

            std::cout << "Sent packet: body_len=" << hdr.body_len << "\n";

            // 接收服务端返回（假设也是 PacketHeader + body）
            PacketHeader resp_hdr{};
            boost::asio::read(socket, boost::asio::buffer(&resp_hdr, sizeof(resp_hdr)));

            std::vector<char> resp_body(resp_hdr.body_len);
            if (resp_hdr.body_len > 0) {
                boost::asio::read(socket, boost::asio::buffer(resp_body));
            }

            std::string resp_str(resp_body.begin(), resp_body.end());
            std::cout << "Response: " << resp_str << "\n";
        }

        socket.close();
        std::cout << "Client exited.\n";

    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }
    return 0;
}
