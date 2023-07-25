cmake_minimum_required(VERSION 3.17)
project(ex3_sadna C)

set(CMAKE_C_STANDARD 11)

add_executable(ex3_sadna
        map.c map.h bw_template.c)
