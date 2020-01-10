set(play_SOURCES
    command_line.cpp
    main.cpp
    server.cpp
    statemachine.cpp
    telnet_service.cpp
    telnet_session.cpp
    )

add_executable(playground ${play_SOURCES})
target_link_libraries(playground ${raft_test_Deps})

set_target_properties(playground PROPERTIES COMPILE_DEFINITIONS ASIO_STANDALONE)