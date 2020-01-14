cmake_minimum_required(VERSION 2.6)
project(data-server C CXX)

set(CMAKE_CXX_COMPILER "g++")
set(CMAKE_CXX_STANDARD 11)

set(CXX_FLAGS
    -std=c++11
    -g
    -Wall
    -Wextra
    -Wformat
    -Wsign-compare
    -Wno-unused-parameter
    -Wno-unused-variable
    -Woverloaded-virtual
    -Wno-missing-field-initializers
    -D__STDC_FORMAT_MACROS
    -DASIO_STANDALONE
)

set(CMAKE_C_COMPILER "gcc")
set(CMAKE_C_STANDARD 99)

set(C_FLAGS
#    -std=gnu99
    -g
    -Wall
    -Wextra
    -Wformat
    -Wsign-compare
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-missing-field-initializers
)

string(REPLACE ";" " " CXX_FLAGS_TEMP "${CXX_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_TEMP}")

string(REPLACE ";" " " C_FLAGS_TEMP "${C_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${C_FLAGS_TEMP}")

#set(CMAKE_C_FLAGS "${C_FLAGS}")

if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -DNDEBUG")
else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -ggdb")
endif()

MESSAGE(STATUS CMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE})

if (APPLE)
    set(CMAKE_MACOSX_RPATH 1)
endif()

add_custom_target(build-3rd)
include(cmake/third-party.cmake)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)

# generate proto
include(cmake/gen-proto.cmake)
include_directories(${proto_INCLUDE_DIR})
add_library(jim-proto STATIC ${proto_SOURCES})

set(SOURCES
    src/server/main.cpp
    src/server/server.cpp
    src/server/worker.cpp
    src/server/node_address.cpp
    src/server/run_status.cpp
    src/server/range_context_impl.cpp
    src/server/range_server.cpp
    src/server/range_tree.cpp
    src/server/rpc_server.cpp
    src/server/version.cpp
    src/range/errors.cpp
    src/range/range.cpp
    src/range/meta_keeper.cpp
    src/range/kv_api.cpp
    src/range/split.cpp
    src/range/peer.cpp
    src/range/submit.cpp
    src/range/txn.cpp
    src/master/client_impl.cpp
    src/admin/admin_server.cpp
    src/admin/get_config.cpp
    src/admin/get_info.cpp
    src/admin/set_config.cpp
    src/admin/profile.cpp
)
add_executable(data-server ${SOURCES})
add_dependencies(data-server jim-proto)

include_directories(${PROJECT_SOURCE_DIR}/src)
include_directories(${PROJECT_SOURCE_DIR}/src/raft/include)

# use gcov
OPTION (ENABLE_COVERAGE "Use gcov" OFF)
MESSAGE(STATUS ENABLE_COVERAGE=${ENABLE_COVERAGE})
if(ENABLE_COVERAGE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fprofile-arcs -ftest-coverage")
endif()

# profiler
OPTION (ENABLE_GPERF "Use gperf" OFF)
MESSAGE(STATUS ENABLE_GPERF=${ENABLE_GPERF})
if(ENABLE_GPERF)
  find_package(GPERF)
  if(GPERF_FOUND)
    message(STATUS "Found gperf library: " ${GPERF_LIBRARY})
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DUSE_GPERF")
  else()
    message(FATAL_ERROR "gperf library not found")
  endif()
endif()

# gcc address sanitize
OPTION (ENABLE_SANITIZE "Use gcc address sanitize" OFF)
MESSAGE(STATUS ENABLE_SANITIZE=${ENABLE_SANITIZE})
if(ENABLE_SANITIZE )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
endif()

foreach(f IN LISTS SOURCES)
    # remove "src/"
    string(SUBSTRING ${f} 4 -1 fname)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"${fname}\"")
endforeach()

add_subdirectory(src/base)
add_subdirectory(src/common)
add_subdirectory(src/raft)
add_subdirectory(src/net)
add_subdirectory(src/db)
add_subdirectory(src/storage)

set (depend_LIBRARYS
    jim-storage
    jim-db
    jim-proto
    jim-raft
    jim-net
    jim-common
    jim-base
    ${MASSTREE_LIBRARY}
    ${PROTOBUF_LIBRARY}
    ${TBB_LIBRARY}
    ${ROCKSDB_LIBRARY}
    ${CPR_LIBRARY}
    ${MURMUR3_LIBRARY}
    curl
    z
    ${JEMALLOC_LIBRARY}
    pthread
    dl
)

message(STATUS "CMAKE_CXX_FLAGS: " ${CMAKE_CXX_FLAGS})
message(STATUS "CMAKE_C_FLAGS: " ${CMAKE_C_FLAGS})
message(STATUS "Depend Libraries: " "${depend_LIBRARYS}")

target_link_libraries(data-server ${depend_LIBRARYS})

OPTION (BUILD_VERSION "build version" ON)
MESSAGE(STATUS BUILD_VERSION=${BUILD_VERSION})
if (BUILD_VERSION)
    set_source_files_properties(src/server/version.cpp PROPERTIES COMPILE_DEFINITIONS "JIM_BUILD_VERSION")
    add_custom_target(
        gen-version ALL
        ${CMAKE_COMMAND} -DPROJECT_SOURCE_DIR=${PROJECT_SOURCE_DIR} -DBUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DBUILD_FLAGS=${CMAKE_CXX_FLAGS} -P ${PROJECT_SOURCE_DIR}/cmake/version.cmake
    )
    add_dependencies(data-server gen-version)
endif()

install(TARGETS data-server RUNTIME DESTINATION bin)

INSTALL(FILES
    client/kv/jim_kv_command.h
    client/kv/jim_kv_common.h
    client/kv/jim_kv_context.h
    client/kv/jim_kv_reply.h
    client/kv/jim_kv_serialize.h
    client/kv/jim_table_client.h
    client/kv/jim_client.h
    client/kv/jim_macro.h
    client/kv/redis_cmd_table.h
    client/jim_log.h
    DESTINATION include/jim-sdk)

INSTALL(FILES
    build/client/libjim-sdk.a
    build/src/base/libjim-base.a
    build/src/common/libjim-common.a
    build/src/net/libjim-net.a
    build/libjim-proto.a
    .external/protobuf/libprotobuf.a
    third-party/cpr/build/lib/libcpr.a
    DESTINATION /usr/lib64/jim-sdk)
OPTION (BUILD_CLIENT "build client" ON)
MESSAGE(STATUS BUILD_CLIENT=${BUILD_CLIENT})
if (BUILD_CLIENT)
    add_subdirectory(client)
endif()

OPTION (BUILD_TEST "build jimdb dataserver tests" OFF)
MESSAGE(STATUS BUILD_TEST=${BUILD_TEST})
if (BUILD_TEST)
    enable_testing()
    add_subdirectory(test)
    add_library(jim-ds-shared SHARED ${SOURCES})
    add_dependencies(jim-ds-shared jim-proto)
    if (APPLE)
        target_link_libraries(jim-ds-shared ${depend_LIBRARYS})
    endif()
endif()

OPTION(BUILD_TOOL "build jimdb dataserver tools" OFF)
MESSAGE(STATUS BUILD_TOOL=${BUILD_TOOL})
if (BUILD_TOOL)
    add_subdirectory(tools)
endif()
