set(raft_test_SOURCES
    single_node_test.cpp
    cluster_test.cpp
)

foreach(f IN LISTS raft_test_SOURCES) 
    # remove "src/" 
    #string(SUBSTRING ${f} 4 -1 fname) 
    set_source_files_properties(${f} PROPERTIES 
        COMPILE_DEFINITIONS "__FNAME__=\"raft/test/${f}\"") 
endforeach() 

add_executable(single_node_test single_node_test.cpp)
target_link_libraries(single_node_test ${raft_test_Deps})

add_executable(cluster_test cluster_test.cpp)
target_link_libraries(cluster_test  ${raft_test_Deps})

add_subdirectory(bench)
add_subdirectory(unittest)
if (RAFT_BUILD_PLAYGROUND) 
    add_subdirectory(playground)
endif()
