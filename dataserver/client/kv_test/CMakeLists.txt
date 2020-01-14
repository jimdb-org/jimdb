include_directories(${PROJECT_SOURCE_DIR}/client)
include_directories(${PROJECT_SOURCE_DIR}/client/kv)
include_directories(${PROJECT_SOURCE_DIR}/client/kv_test)

set(kv_test_SRCS
    kv_test.cpp
    kv_reply.cpp
    kv_hash.cpp
    kv_string.cpp
)

set (kv_test_DEPS
    jim-sdk
    jim-base
    jim-common
    jim-proto
    jim-net
    ${PROTOBUF_LIBRARY}
    ${CPR_LIBRARY}
    ${JEMALLOC_LIBRARY}
    pthread
    curl
    dl
)

foreach(f IN LISTS kv_test_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

set_source_files_properties(kv_scan_decode.cpp PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"kv_scan_decode.cpp\"")

add_executable(kv_test ${kv_test_SRCS})
target_link_libraries(kv_test ${kv_test_DEPS})
#target_link_libraries(log_dump ${log_dump_DEPS})

add_executable(kv_scan_decode kv_scan_decode.cpp)
target_link_libraries(kv_scan_decode ${kv_test_DEPS})
