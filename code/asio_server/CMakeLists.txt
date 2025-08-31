cmake_minimum_required(VERSION 3.10)

# 设置项目名称和语言
project(asio_server)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include_directories(/home/tuxu/Source/asio/include)

# 查找 Boost 库，确保安装了 Boost 系统库和线程库
find_package(Boost REQUIRED COMPONENTS system thread)

# 设置编译选项
add_executable(asio_server asio_server.cpp)

# 链接 Boost 库
target_link_libraries(asio_server
        PRIVATE
            Boost::system
            Boost::thread
            pthread
)

