set(raft_bench_SOURCES
    address.cpp
    config.cpp
    main.cpp
    node.cpp
    range.cpp
    )

foreach(f IN LISTS raft_bench_SOURCES) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"raft/test/bench/${f}\"") 
endforeach() 
add_executable(raft-bench ${raft_bench_SOURCES})
target_link_libraries(raft-bench ${raft_test_Deps})
