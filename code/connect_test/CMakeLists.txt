cmake_minimum_required(VERSION 3.10)

project(tcp_client_demo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(connect_test connect_test.cpp)

# Linux 上需要链接 pthread 才能保证某些系统调用正常工作（例如 pause/signal）
target_link_libraries(connect_test PRIVATE pthread)
