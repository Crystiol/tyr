cmake_minimum_required(VERSION 3.10)
project(lt_server_echo CXX)

# 使用 C++11（你的代码是基于 C++11 编写的）
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 打开优化和警告
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O3 -pthread")

# 可执行文件
add_executable(lt_server_echo lt_server_echo.cpp
)

# 需要 pthread
find_package(Threads REQUIRED)
target_link_libraries(lt_server_echo PRIVATE Threads::Threads)

