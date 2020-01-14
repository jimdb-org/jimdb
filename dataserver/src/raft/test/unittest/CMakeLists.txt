add_library(jim-raft-testutil STATIC test_util.cpp)

set(raft_unit_DEPS
    jim-raft-testutil
    jim-raft
    jim-base
    jim-proto
    ${GTEST_LIBRARY}
    ${PROTOBUF_LIBRARY}
    pthread
)

set (raft_unit_TESTS
    disk_storage_unittest.cpp
    log_file_unittest.cpp
    meta_file_unittest.cpp
    replica_unittest.cpp
    raft_log_unittest.cpp
    raft_types_unittest.cpp
    log_unstable_unittest.cpp
    snapshot_send_unittest.cpp
    snapshot_worker_unittest.cpp
)

ENABLE_TESTING()
foreach(testfile ${raft_unit_TESTS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${raft_unit_DEPS})
    target_include_directories(${exename} PUBLIC ${GTEST_INCLUDE_DIR} ${GMOCK_INCLUDE_DIR})
    add_test(NAME ${exename} COMMAND ${exename})
endforeach(testfile ${TESTS})
