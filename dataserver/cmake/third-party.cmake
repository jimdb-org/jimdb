include(ExternalProject)

set(EXTERNAL_PATH "${PROJECT_SOURCE_DIR}/.external" CACHE PATH "external deps path")

set(CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/external)

include(github/repos)

include(asio)
include(tbb)
include(protobuf)
include(rocksdb)
include(cpr)
include(murmur3)
include(jemalloc)
include(masstree)

# gtest and benchmark
if (BUILD_BENCH)
    include(gtest)
    include(benchmark)
    add_subdirectory(${PROJECT_SOURCE_DIR}/third-party/sql-parser)
elseif(BUILD_TEST)
    include(gtest)
    add_subdirectory(${PROJECT_SOURCE_DIR}/third-party/sql-parser)
elseif(BUILD_RAFT_TEST)
    include(gtest)
endif()

# third-party header only
include_directories(${PROJECT_SOURCE_DIR}/third-party)
include_directories(${PROJECT_SOURCE_DIR}/third-party/rapidjson-1.1.0/include)
include_directories(${PROJECT_SOURCE_DIR}/third-party/spdlog-1.3.1/include)
include_directories(${PROJECT_SOURCE_DIR}/third-party/inih)
