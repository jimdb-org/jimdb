include_directories(${PROJECT_SOURCE_DIR}/test)
include_directories(${PROJECT_SOURCE_DIR}/third-party/sql-parser/src)

set(test_helper_SRCS
    helper/mock/range_context_mock.cpp
    helper/mock/rpc_request_mock.cpp
    helper/helper_util.cpp
    helper/request_builder.cpp
    helper/sql_parser.cpp
    helper/query_parser.cpp
    helper/range_test_fixture.cpp
    helper/store_test_fixture.cpp
    helper/table.cpp
)

foreach(f IN LISTS test_helper_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

add_library(jimkv-test-helper SHARED ${test_helper_SRCS})
target_include_directories(jimkv-test-helper PUBLIC ${GTEST_INCLUDE_DIR} ${GMOCK_INCLUDE_DIR})
if (APPLE)
    target_link_libraries(jimkv-test-helper jimkv-ds-shared sqlparser ${GTEST_LIBRARY})
endif()

set (test_DEPEND_LIBS
    jimkv-test-helper
    jimkv-ds-shared
    ${depend_LIBRARYS}
    sqlparser
    ${GTEST_LIBRARY}
)

set(test_SRCS
    unittest/checkpoint_unittest.cpp
    unittest/db_unittest.cpp
    unittest/encoding_unittest.cpp
    unittest/field_value_unittest.cpp
    unittest/mass_tree_unittest.cpp
    unittest/meta_store_unittest.cpp
    unittest/monitor_unittest.cpp
    unittest/range_ddl_unittest.cpp
    unittest/range_meta_unittest.cpp
    unittest/range_kv_unittest.cpp
    unittest/range_selectflow_unittest.cpp
    unittest/range_sql_unittest.cpp
    unittest/range_tree_unittest.cpp
    unittest/scaner_unittest.cpp
    unittest/status_unittest.cpp
    unittest/store_unittest.cpp
    unittest/store_txn_unittest.cpp
    unittest/timer_unittest.cpp
    unittest/util_unittest.cpp
    unittest/wal_unittest.cpp
    unittest/range_flow_unittest.cpp
    unittest/range_stats_unittest.cpp
    unittest/range_selectflow2_unittest.cpp
    unittest/range_selectflow_arithmetic_unittest.cpp
    unittest/range_selectflow_cmp_unittest.cpp
    unittest/store_flow_unittest.cpp
    unittest/store_flow_double_unittest.cpp
    unittest/range_selectflow_datasample_unittest.cpp
    unittest/histogram_test.cpp
    unittest/cm_sketch_test.cpp
    unittest/fm_sketch_test.cpp
    unittest/datatype_decimal_unittest.cpp
    unittest/range_selectflow_decimal_unittest.cpp
    unittest/datatype_datetime_unittest.cpp
    unittest/datatype_time_unittest.cpp
    unittest/range_selectflow_datetime_unittest.cpp
    unittest/range_selectflow_time_unittest.cpp
)

foreach(f IN LISTS test_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

set(TEST_RUN_ARGS "" CACHE STRING "unittest run args")

foreach(testfile ${test_SRCS})
    get_filename_component(exename ${testfile} NAME_WE)
    add_executable(${exename} ${testfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    target_include_directories(${exename} PUBLIC ${GTEST_INCLUDE_DIR} ${GMOCK_INCLUDE_DIR})
    if("${exename}" MATCHES "unittest")
        add_test(NAME ${exename} COMMAND ${exename} ${TEST_RUN_ARGS})
    endif()
endforeach(testfile ${TESTS})

#if (BUILD_BENCH)
#    add_subdirectory(bench)
#endif()

set(bench_SRCS
    bench/db_bench.cpp
    bench/storage_bench.cpp
)

foreach(f IN LISTS bench_SRCS)
    set_source_files_properties(${f} PROPERTIES
            COMPILE_DEFINITIONS "__FNAME__=\"${f}\"")
endforeach()

set(BENCH_RUN_ARGS "" CACHE STRING "bench run args")

foreach(benchfile ${bench_SRCS})
    get_filename_component(exename ${benchfile} NAME_WE)
    add_executable(${exename} ${benchfile})
    target_link_libraries(${exename} ${test_DEPEND_LIBS})
    target_include_directories(${exename} PUBLIC ${GTEST_INCLUDE_DIR} ${GMOCK_INCLUDE_DIR})
    if("${exename}" MATCHES "bench")
        add_test(NAME ${exename} COMMAND ${exename} ${BENCH_RUN_ARGS})
    endif()
endforeach(benchfile ${BENCHS})
