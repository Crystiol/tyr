// Client4.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

// Client2.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

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
#include <iomanip>

#include "TcpClient.h"
#include "htime.h"
#include "spdlog/cfg/env.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

#define TEST_RECONNECT 1
#define TEST_TLS 0

using namespace hv;

#pragma pack(push, 1)
struct PacketHeader {
  uint8_t body_offset;
  uint32_t body_len;
};
#pragma pack(pop)

#define TEST_CNT    1000*1000*1000

std::shared_ptr<spdlog::logger> logger_;
void init_logger() {
  auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  console_sink->set_level(spdlog::level::debug);
  console_sink->set_pattern("[et_server] [%^%l%$] %v");

  auto file_sink =
      std::make_shared<spdlog::sinks::basic_file_sink_mt>("cli_log.txt", true);
  file_sink->set_level(spdlog::level::trace);

  logger_ = std::make_shared<spdlog::logger>(
      "multi_sink", spdlog::sinks_init_list{console_sink, file_sink});
  logger_->set_level(spdlog::level::debug);
  logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [tid %t] [%^%l%$] %v");
}

std::atomic<uint64_t> nConnError = 0;
std::atomic<uint64_t> nReadError = 0;
std::atomic<uint64_t> nWriteError = 0;
std::atomic_bool bContinue = false;
std::chrono::system_clock::time_point start_time;

int64_t seq = 0;
std::atomic<uint64_t> recv_cnt = 0;

#define SEND_SPEED      20000

int main(int argc, char *argv[]) {
  if (argc < 2) {
    printf("Usage: %s remote_port [remote_host]\n", argv[0]);
    return -10;
  }
  int remote_port = atoi(argv[1]);
  const char *remote_host = "192.168.169.130";
  if (argc > 2) {
    remote_host = argv[2];
  }

  init_logger();
  

  std::thread *works[8];
  for (int i = 0; i < 1; i++) {
    works[i] = new std::thread([&] {
      TcpClient *cli = new TcpClient;
      int connfd = cli->createsocket(remote_port, remote_host);
      if (connfd < 0) {
        return -20;
      }
      tcp_nodelay(connfd, 1);
      logger_->debug("client connect to port {}, connfd={} ...", remote_port, connfd);
      cli->onConnection = [cli](const SocketChannelPtr &channel) {
        std::string peeraddr = channel->peeraddr();
        if (channel->isConnected()) {
            logger_->debug("connected to {}! connfd={}\n", peeraddr.c_str(), channel->fd());
            if (channel->isConnected()) {
                if (channel->isWriteComplete()) {
                int i = SEND_SPEED;
                while(--i >= 0)
                {
                    std::stringstream ss;
                    ss << std::setw(8) << std::left << ++seq;
                    std::string body = ss.str();

                    size_t data_len = body.size();
                    PacketHeader hdr{};
                    hdr.body_offset = 5;
                    hdr.body_len = static_cast<uint32_t>(data_len);

                    std::string packet;
                    packet.append((char *)&hdr, sizeof(hdr));
                    packet.append(body.data(), data_len);

                    logger_->debug("send data: {}", body.c_str());
                    channel->write(packet);
                }

                start_time = std::chrono::system_clock::now();
               }
            }
        } else {
          // printf("disconnected to %s! connfd=%d\n", peeraddr.c_str(),
          // channel->fd());
        }
        if (cli->isReconnect()) {
          printf("reconnect cnt=%d, delay=%d\n",
                 cli->reconn_setting->cur_retry_cnt,
                 cli->reconn_setting->cur_delay);
        }
      };
      cli->onMessage = [](const SocketChannelPtr &channel, Buffer *buf) {
        recv_cnt.fetch_add(buf->len);
        logger_->debug("recv {} bytes: {}", recv_cnt.load(), (char*)buf->data());
        

        bContinue.store(false);

        auto end_time = std::chrono::system_clock::now();
        uint64_t elapse = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
        
        int i = SEND_SPEED;
        while (recv_cnt.load()%(8*SEND_SPEED) == 0 && seq < TEST_CNT && --i >= 0)
        {
            std::stringstream ss;
            ss << std::setw(8) << std::left << ++seq;
            std::string body = ss.str();

            size_t data_len = body.size();
            PacketHeader hdr{};
            hdr.body_offset = 5; // 魔数
            hdr.body_len = static_cast<uint32_t>(data_len);

            std::string packet;
            packet.append((char*)&hdr, sizeof(hdr));
            packet.append(body.data(), data_len);

            logger_->debug("send data: {}", body.c_str());
            channel->write(packet);

            bContinue.store(true);
        }

        /*if (seq == TEST_CNT){
            channel->close();

            auto end_time = std::chrono::system_clock::now();
            uint64_t elapse = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
            logger_->debug(
                "nConnError = {}, nWriteError = {}, nReadError = {}, elapse = {}",
                nConnError.load(), nWriteError.load(), nReadError.load(), elapse);
            return;
        }*/
      };
      cli->start();
    });
  }

  for (int i = 0; i < 1; i++) {
    if (works[i]->joinable())
      works[i]->join();
  }

  auto end_time = std::chrono::system_clock::now();
  uint64_t elapse = std::chrono::duration_cast<std::chrono::milliseconds>(
                        end_time - start_time)
                        .count();
  logger_->debug(
      "nConnError = {}, nWriteError = {}, nReadError = {}, elapse = {}",
      nConnError.load(), nWriteError.load(), nReadError.load(), elapse);

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
//   5.
//   转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门使用技巧:
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5.
//   转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件

// 运行程序: Ctrl + F5 或调试 >“开始执行(不调试)”菜单
// 调试程序: F5 或调试 >“开始调试”菜单

// 入门使用技巧:
//   1. 使用解决方案资源管理器窗口添加/管理文件
//   2. 使用团队资源管理器窗口连接到源代码管理
//   3. 使用输出窗口查看生成输出和其他消息
//   4. 使用错误列表窗口查看错误
//   5.
//   转到“项目”>“添加新项”以创建新的代码文件，或转到“项目”>“添加现有项”以将现有代码文件添加到项目
//   6. 将来，若要再次打开此项目，请转到“文件”>“打开”>“项目”并选择 .sln 文件
