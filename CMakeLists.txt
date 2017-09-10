cmake_minimum_required(VERSION 3.8)
project(G2DB)

set(CMAKE_C_STANDARD 11)

set(SOURCE_FILES g2db.c)
add_executable(G2DB ${SOURCE_FILES})