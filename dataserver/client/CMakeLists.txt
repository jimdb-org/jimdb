set(client_SOURCES
    jim_assist.cpp
    jim_client.cpp
    jim_cluster.cpp
    jim_conn.cpp
    jim_hash.cpp
    jim_master.cpp
    jim_message.cpp
    jim_node.cpp
    jim_route.cpp
    jim_string.cpp
    jim_log.c

    kv/jim_kv_command.cpp
    kv/jim_kv_context.cpp
    kv/jim_kv_reply.cpp
    kv/jim_kv_serialize.cpp
    kv/jim_table_client.cpp
    kv/redis_cmd_table.cpp
    kv/jimdb_client.cpp
)

foreach(f IN LISTS client_SOURCES)
    set_source_files_properties(${f} PROPERTIES
        COMPILE_DEFINITIONS "__FNAME__=\"client/${f}\"")
endforeach()

include_directories(${PROJECT_SOURCE_DIR}/client)
include_directories(${PROJECT_SOURCE_DIR}/src/base)

add_library(jim-sdk STATIC ${client_SOURCES})
add_dependencies(jim-sdk jim-proto)

if (BUILD_TEST)
    add_subdirectory(kv_test)
endif()

if (BUILD_TOOL)
    add_subdirectory(tool)
endif()
