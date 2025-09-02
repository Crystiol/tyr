// Client2.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

/*
 * TcpClient_test.cpp
 *
 * @build   make evpp
 * @server  bin/TcpServer_test 1234
 * @client  bin/TcpClient_test 1234
 *
 */

#include <iostream>

#include "TcpClient.h"
#include "htime.h"

#define TEST_RECONNECT  1
#define TEST_TLS        0

using namespace hv;

#pragma pack(push, 1)
struct PacketHeader {
    uint8_t  body_offset;
    uint32_t body_len;
};
#pragma pack(pop)

int seq = 0;
int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s remote_port [remote_host]\n", argv[0]);
        return -10;
    }
    int remote_port = atoi(argv[1]);
    const char* remote_host = "192.168.163.130";
    if (argc > 2) {
        remote_host = argv[2];
    }

    std::thread* works[8];
    for (int i = 0; i < 1; i++) {
        works[i] = new std::thread([&] {
            int loop_cnt = 10;
            while (--loop_cnt >= 0) {
                TcpClient cli;
                int connfd = cli.createsocket(remote_port, remote_host);
                if (connfd < 0) {
                    return -20;
                }
                printf("client connect to port %d, connfd=%d ...\n", remote_port, connfd);
                cli.onConnection = [&cli](const SocketChannelPtr& channel) {
                    std::string peeraddr = channel->peeraddr();
                    if (channel->isConnected()) {
                        printf("connected to %s! connfd=%d\n", peeraddr.c_str(), channel->fd());
                        if (channel->isConnected()) {
                            if (channel->isWriteComplete()) {
                                std::string body = std::to_string(++seq);
                                size_t data_len = body.size();
                                PacketHeader hdr{};
                                hdr.body_offset = 5;  // 魔数
                                hdr.body_len = static_cast<uint32_t>(data_len);

                                std::string packet;
                                packet.append((char*)&hdr, sizeof(hdr));
                                packet.append(body.data(), data_len);

                                printf("send data: %s\n", body.c_str());
                                channel->write(packet);
                            }
                        }
                    }
                    else {
                        //printf("disconnected to %s! connfd=%d\n", peeraddr.c_str(), channel->fd());
                    }
                    if (cli.isReconnect()) {
                        printf("reconnect cnt=%d, delay=%d\n", cli.reconn_setting->cur_retry_cnt, cli.reconn_setting->cur_delay);
                    }
                };
                cli.onMessage = [](const SocketChannelPtr& channel, Buffer* buf) {
                    //printf("< %.*s\n", (int)buf->size(), (char*)buf->data());
                    std::cout << "received : " << (char*)buf->data() + 5 << "\n";
                    channel->close();
                };
                cli.start();
            }
            });
    }

    std::string str;
    while (std::getline(std::cin, str)) {
    }
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
