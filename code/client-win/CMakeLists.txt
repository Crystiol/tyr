cmake_minimum_required(VERSION 3.15)
project(ClientWin LANGUAGES CXX)

# 要求 C++17
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 如果是 MSVC，启用常见优化选项
if (MSVC)
    add_compile_options(/W4 /permissive-)
else()
    add_compile_options(-Wall -Wextra -O2)
endif()

# 可执行文件
add_executable(client_win client3.cpp)

# Windows 需要链接 ws2_32 库
if (WIN32)
    target_link_libraries(client_win PRIVATE ws2_32)
endif()
