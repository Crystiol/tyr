cmake_minimum_required(VERSION 3.10)
project(boost_client_demo LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS system)
find_package(ZLIB REQUIRED)

add_executable(client client.cpp)

target_link_libraries(client
    PRIVATE
        Boost::system
        ZLIB::ZLIB
)
