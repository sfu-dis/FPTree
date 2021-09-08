cmake_minimum_required(VERSION 3.14)
include(FetchContent)
project(fptree)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE Release)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -std=c++17 -lpmem -lpmemobj -ljemalloc -mavx512f -mavx512vl -mavx512bw -mavx512dq -mavx512cd -mrtm -ltbb -pthread")

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/src)

set(PMEM_BACKEND "PMEM" CACHE STRING "Persistent memory backend type")

if(${PMEM_BACKEND} STREQUAL "PMEM")
  add_definitions(-DPMEM)
  message(STATUS "Persistence support: PMEM")
elseif(${PMEM_BACKEND} STREQUAL "DRAM")
  message(STATUS "Persistence support: off")
else()
  message(FATAL_ERROR "Unsupported persistent memory backend: ${PMEM_BACKEND}")
endif()

option(BUILD_INSPECTOR "Build inspector to check correctness of single/multi-thread operation" ON)

option(TEST_MODE "Test mode will test leaf nodes and inner node smaller to help debug" ON)

option(NDEBUG "Disable assert statements" ON)


if(${TEST_MODE})
  add_definitions(-DTEST_MODE)
  message(STATUS "TEST_MODE: defined")
else()
  message(STATUS "TEST_MODE: not defined")
endif()

if(${NDEBUG})
  add_definitions(-DNDEBUG)
  message(STATUS "NDEBUG: defined")
else()
  message(STATUS "NDEBUG: not defined")
endif()

if(${BUILD_INSPECTOR})
  add_definitions(-DBUILD_INSPECTOR)
  message(STATUS "BUILD_INSPECTOR: defined")
  add_executable(
    inspector
    fptree.cpp
    fptree.h
    inspector.cpp
  )
else()
  message(STATUS "BUILD_INSPECTOR: not defined")
  add_executable(
    fptree
    fptree.cpp
    fptree.h
  )
endif()

##################### PiBench #########################
FetchContent_Declare(
        pibench 
        GIT_REPOSITORY https://github.com/sfu-dis/pibench.git
        GIT_TAG master 
)
if (NOT pibench_POPULATED)
  FetchContent_Populate(pibench)
  include_directories(${pibench_SOURCE_DIR}/include)
endif ()


add_library(fptree_pibench_wrapper SHARED fptree_wrapper.cpp
						fptree.cpp)

