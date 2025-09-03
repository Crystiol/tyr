// Client1.cpp : 此文件包含 "main" 函数。程序执行将在此处开始并结束。
//

#include <iostream>
#include <string>
#include <vector>
#include <cstdint>
#include <cstring>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <chrono>
#include <ctime>
#include <iomanip>
#include "spdlog/spdlog.h"
#include "spdlog/cfg/env.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/basic_file_sink.h"
#pragma comment(lib, "ws2_32.lib")  // 链接 Winsock 库

using namespace std::chrono_literals;

// 协议头
#pragma pack(push, 1)
struct PacketHeader {
	uint8_t  body_offset;
	uint32_t body_len;
};
#pragma pack(pop)

std::shared_ptr<spdlog::logger> logger_;
void init_logger() {
	auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
	console_sink->set_level(spdlog::level::debug);
	console_sink->set_pattern("[et_server] [%^%l%$] %v");

	auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>("et_log.txt", true);
	file_sink->set_level(spdlog::level::trace);

	logger_ = std::make_shared<spdlog::logger>("multi_sink", spdlog::sinks_init_list{ console_sink, file_sink });
	logger_->set_level(spdlog::level::debug);
	logger_->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [tid %t] [%^%l%$] %v");
}

// CRC32 计算
static inline uint32_t crc32_calc(const void* data, size_t len) {
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
	const uint8_t* p = reinterpret_cast<const uint8_t*>(data);
	for (size_t i = 0; i < len; ++i)
		c = table[(c ^ p[i]) & 0xFFu] ^ (c >> 8);
	return c ^ 0xFFFFFFFFu;
}

void print_time()
{
	SYSTEMTIME st;
	GetLocalTime(&st);  // 获取本地时间，精确到毫秒

	std::cout << std::setfill('0')
		<< st.wYear << '-'
		<< std::setw(2) << st.wMonth << '-'
		<< std::setw(2) << st.wDay << ' '
		<< std::setw(2) << st.wHour << ':'
		<< std::setw(2) << st.wMinute << ':'
		<< std::setw(2) << st.wSecond << '.'
		<< std::setw(3) << st.wMilliseconds
		<< std::endl;
}

int seq = 0;

int main() {
	const char* server_ip = "192.168.163.130";
	const int server_port = 9000;

	// 初始化 Winsock
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
		std::cerr << "WSAStartup failed: " << WSAGetLastError() << "\n";
		return 1;
	}


	init_logger();
	auto start_time = std::chrono::system_clock::now();

	std::atomic<uint64_t> nConnError = 0;
	std::atomic<uint64_t> nReadError = 0;
	std::atomic<uint64_t> nWriteError = 0;
	std::thread* works[8];
	for (int i = 0; i < 1; i++)
	{
		works[i] = new std::thread([&] {
			int loop_cnt = 5000;
			while (--loop_cnt >= 0) {
				// 创建 socket
				SOCKET sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
				if (sock == INVALID_SOCKET) {
					logger_->debug("socket failed: {}", WSAGetLastError());
					return 1;
				}

				// 允许端口快速复用（TIME_WAIT 时可重用）
				BOOL reuse = TRUE;
				if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(reuse)) < 0) {
					logger_->debug("setsockopt(SO_REUSEADDR) failed: {}", WSAGetLastError());
					return 1;
				}

				//sockaddr_in local{};
				//local.sin_family = AF_INET;
				//local.sin_addr.s_addr = htonl(INADDR_ANY);
				//local.sin_port = htons(50000); // 固定端口测试
				//if (bind(sock, (sockaddr*)&local, sizeof(local)) < 0) {
				//	std::cerr << "bind() failed: " << WSAGetLastError() << "\n";
				//	WSACleanup();
				//	return 1;
				//}

				// 设置服务器地址
				sockaddr_in server_addr{};
				server_addr.sin_family = AF_INET;
				server_addr.sin_port = htons(server_port);
				if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
					logger_->debug("inet_pton failed: {}", WSAGetLastError());
					closesocket(sock);
					return 1;
				}

				// 连接服务器
				logger_->debug("connected to server");
				if (connect(sock, (sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
					nConnError.fetch_add(1, std::memory_order_relaxed);
					logger_->debug("connect failed: {}", WSAGetLastError());
					closesocket(sock);
					return 1;
				}
				
				std::string body = std::to_string(++seq);

				// 构造协议头
				PacketHeader hdr{};
				hdr.body_offset = 5;  // 魔数
				hdr.body_len = static_cast<uint32_t>(body.size());

				std::vector<uint8_t> packet(sizeof(hdr) + body.size());
				memcpy(packet.data(), &hdr, sizeof(hdr));
				memcpy(packet.data() + sizeof(hdr), body.data(), body.size());

				logger_->debug("send to server");
				int sent = send(sock, reinterpret_cast<const char*>(packet.data()), (int)packet.size(), 0);
				if (sent == SOCKET_ERROR) {
					nWriteError.fetch_add(1, std::memory_order_relaxed);
					logger_->debug("send failed: {}", WSAGetLastError());
					closesocket(sock);
					return 1;
				}
				logger_->debug("send {} bytes: {}", sent, seq);


				logger_->debug("recv from server");
				// 接收数据
				char buf[1024] = {0};
				int n = recv(sock, buf, sizeof(buf) - 1, 0);
				if (n == SOCKET_ERROR) {
					nReadError.fetch_add(1, std::memory_order_relaxed);
					logger_->debug("recv failed: {}", WSAGetLastError());
				}
				else if (n > 0) {
					logger_->debug("recv {} bytes: {}", n, buf);
				}

				// 关闭连接
				closesocket(sock);
			} 
			});
	}

	for (int i = 0; i < 1; i++)
	{
		if (works[i]->joinable())
			works[i]->join();
	}

	print_time();
	auto end_time = std::chrono::system_clock::now();
	uint64_t elapse = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
	std::cout << "nConnError = " << nConnError 
		<< " , nWriteError = " << nWriteError 
		<< " , nReadError = " << nReadError 
		<< " , elapse = " << elapse
		<< std::endl;

	while (true)
	{
		std::this_thread::sleep_for(1s);
	}

	// 清理 Winsock
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
