set(client_SOURCES
    jimkv_assist.cpp
    jimkv_client.cpp
    jimkv_cluster.cpp
    jimkv_conn.cpp
    jimkv_hash.cpp
    jimkv_master.cpp
    jimkv_message.cpp
    jimkv_node.cpp
    jimkv_route.cpp
    jimkv_string.cpp
    jimkv_log.c

    kv/jimkv_kv_command.cpp
    kv/jimkv_kv_context.cpp
    kv/jimkv_kv_reply.cpp
    kv/jimkv_kv_serialize.cpp
    kv/jimkv_table_client.cpp
    kv/redis_cmd_table.cpp
    kv/jimdb_client.cpp
)

foreach(f IN LISTS client_SOURCES)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"client/${f}\"")
endforeach()

include_directories(${PROJECT_SOURCE_DIR}/client)
include_directories(${PROJECT_SOURCE_DIR}/src/base)

add_library(jimkv-sdk STATIC ${client_SOURCES})
add_dependencies(jimkv-common jimkv-proto)

if (BUILD_TEST)
    add_subdirectory(kv_test)
endif()

if (BUILD_TOOL)
    add_subdirectory(tool)
endif()
