ExternalProject_Add(linenoise
        PREFIX linenoise
        SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/linenoise
        BUILD_IN_SOURCE 0
        INSTALL_COMMAND ""
        )

ExternalProject_Get_Property(linenoise BINARY_DIR)
set(linenoise_LIBRARY ${BINARY_DIR}/liblinenoise.a)

set(console_SRCS
    main.cpp
    client.cpp
    )
add_executable(console ${console_SRCS})
add_dependencies(console linenoise)

set (console_DEPS
    jim-proto
    jim-net
    jim-base
    ${PROTOBUF_LIBRARY}
    ${linenoise_LIBRARY}
    pthread
    z
    )
target_link_libraries(console ${console_DEPS})
