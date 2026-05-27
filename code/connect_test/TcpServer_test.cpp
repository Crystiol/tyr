/*
 * TcpServer_test.cpp
 *
 * @build   make evpp
 * @server  bin/TcpServer_test 1234
 * @client  bin/TcpClient_test 1234
 *
 */

#include <iostream>

#include "TcpServer.h"
#include "htime.h"

using namespace hv;

#define TEST_TLS        0

std::atomic<uint64_t> connection_count{0};

int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Usage: %s port\n", argv[0]);
        return -10;
    }
    int port = atoi(argv[1]);

    char logfile[] = "hlog_test.log";
    hlog_set_file(logfile);
    hlog_set_level(LOG_LEVEL_INFO);
    hlog_set_max_filesize_by_str("100M");

    //hlog_set_level(LOG_LEVEL_DEBUG);

    TcpServer srv;
    int listenfd = srv.createsocket(port);
    if (listenfd < 0) {
        return -20;
    }

    unpack_setting_t grpc_unpack_setting;
    memset(&grpc_unpack_setting, 0, sizeof(unpack_setting_t));
    grpc_unpack_setting.mode = UNPACK_BY_LENGTH_FIELD;
    grpc_unpack_setting.package_max_length = DEFAULT_PACKAGE_MAX_LENGTH;
    grpc_unpack_setting.body_offset = 9;
    grpc_unpack_setting.length_field_offset = 5;
    grpc_unpack_setting.length_field_bytes = 4;
    grpc_unpack_setting.length_field_coding = ENCODE_BY_LITTEL_ENDIAN;
    srv.setUnpack(&grpc_unpack_setting);

    LOGI("server listen on port %d, listenfd=%d ...\n", port, listenfd);
    srv.onConnection = [](const SocketChannelPtr& channel) {
        std::string peeraddr = channel->peeraddr();
        if (channel->isConnected()) {
            uint64_t val = connection_count.fetch_add(1, std::memory_order_relaxed);
            LOGI("connections = %llu\n", val);
            //printf("%s connected! connfd=%d id=%d tid=%ld\n", peeraddr.c_str(), channel->fd(), channel->id(), currentThreadEventLoop->tid());
        } else {
            printf("%s disconnected! connfd=%d id=%d tid=%ld\n", peeraddr.c_str(), channel->fd(), channel->id(), currentThreadEventLoop->tid());
        }
    };
    srv.onMessage = [](const SocketChannelPtr& channel, Buffer* buf) {
        // echo
        printf("< %.*s\n", (int)buf->size()-9, (char*)buf->data()+9);
        channel->write(buf);
    };
    srv.setThreadNum(4);

    srv.setLoadBalance(LB_LeastConnections);

#if TEST_TLS
    hssl_ctx_opt_t ssl_opt;
    memset(&ssl_opt, 0, sizeof(hssl_ctx_opt_t));
    ssl_opt.crt_file = "cert/server.crt";
    ssl_opt.key_file = "cert/server.key";
    ssl_opt.verify_peer = 0;
    srv.withTLS(&ssl_opt);
#endif

    srv.start();

    std::string str;
    while (std::getline(std::cin, str)) {
        if (str == "close") {
            srv.closesocket();
        } else if (str == "start") {
            srv.start();
        } else if (str == "stop") {
            srv.stop();
            break;
        } else {
            srv.broadcast(str.data(), str.size());
        }
    }

    return 0;
}
