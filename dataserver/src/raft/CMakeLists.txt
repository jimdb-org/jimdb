set(raft_SOURCES
    src/impl/bulletin_board.cpp
    src/impl/mutable_options.cpp
    src/impl/raft_fsm_candidate.cpp
    src/impl/raft_fsm.cpp
    src/impl/raft_fsm_follower.cpp
    src/impl/raft_fsm_leader.cpp
    src/impl/raft_impl.cpp
    src/impl/raft_read_context.cpp
    src/impl/raft_log.cpp
    src/impl/raft_log_unstable.cpp
    src/impl/raft.pb.cc
    src/impl/raft_types.cpp
    src/impl/replica.cpp
    src/impl/server_impl.cpp
    src/impl/snapshot/apply_task.cpp
    src/impl/snapshot/manager.cpp
    src/impl/snapshot/send_task.cpp
    src/impl/snapshot/worker.cpp
    src/impl/snapshot/worker_pool.cpp
    src/impl/storage/log_file.cpp
    src/impl/storage/log_format.cpp
    src/impl/storage/log_index.cpp
    src/impl/storage/log_reader.cpp
    src/impl/storage/meta_file.cpp
    src/impl/storage/storage_disk.cpp
    src/impl/storage/storage_memory.cpp
    src/impl/transport/inprocess_transport.cpp
    src/impl/transport/tcp_transport.cpp
    src/impl/work_thread.cpp
    src/options.cpp
    src/server.cpp
    src/status.cpp
    src/types.cpp
)

foreach(f IN LISTS raft_SOURCES)
    # remove "src/"
    string(SUBSTRING ${f} 4 -1 fname)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"raft/${fname}\"")
endforeach()

add_library(jim-raft STATIC ${raft_SOURCES})
add_dependencies(jim-raft build-3rd)

set(raft_test_Deps
     jim-raft
     jim-net
     jim-base
     jim-common
     ${PROTOBUF_LIBRARY}
     ${JEMALLOC_LIBRARY}
     pthread
     )

OPTION(JIMKV_RAFT_TRACE_MSG "raft trace log" OFF)
MESSAGE(STATUS JIMKV_RAFT_TRACE_MSG=${JIMKV_RAFT_TRACE_MSG})
if (JIMKV_RAFT_TRACE_MSG)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DJIMKV_RAFT_TRACE_MSG")
endif()

OPTION(BUILD_RAFT_TEST "build raft tests" OFF)
MESSAGE(STATUS BUILD_RAFT_TEST=${BUILD_RAFT_TEST})
if (BUILD_RAFT_TEST)
    add_subdirectory(test)
endif()
