cmake_minimum_required(VERSION 3.22)
project(micro_bench)

set(CMAKE_CXX_STANDARD 20)

add_executable(micro_bench main.cpp
        src/types.hpp
        src/storage/mvcc_data.cpp
        src/storage/mvcc_data.hpp)
