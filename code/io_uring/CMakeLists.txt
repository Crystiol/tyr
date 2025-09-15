cmake_minimum_required(VERSION 3.10)
project(io_uring_server CXX)

# 使用 C++11（你的代码是基于 C++11 编写的）
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE "Debug")

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3 -pthread")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -pthread")

include_directories(/home/tuxu/3rdparty/include)
link_directories(/home/tuxu/3rdparty/lib)

# 可执行文件
add_executable(io_uring_server
        io_uring_server.cpp
)

# 需要 pthread
find_package(Threads REQUIRED)
target_link_libraries(io_uring_server PRIVATE Threads::Threads uring)

