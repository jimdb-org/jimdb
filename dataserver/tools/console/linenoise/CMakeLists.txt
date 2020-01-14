cmake_minimum_required(VERSION 2.6)
project(linenoise C)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -W -Os -g")
add_library(linenoise linenoise.c)
