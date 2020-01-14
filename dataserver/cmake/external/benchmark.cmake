# benchmark
set(BENCHMARK_TAG v1.5.0)
set(BENCHMARK_INCLUDE_DIR ${EXTERNAL_PATH}/benchmark/include)
set(BENCHMARK_LIBRARY ${EXTERNAL_PATH}/benchmark/src/libbenchmark.a)
ExternalProject_Add(benchmark
        PREFIX benchmark
        GIT_REPOSITORY ${BENCHMARK_URL}
        GIT_TAG ${BENCHMARK_TAG}
        SOURCE_DIR ${EXTERNAL_PATH}/benchmark
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND cmake . -DCMAKE_BUILD_TYPE=Release -DGOOGLETEST_PATH=${EXTERNAL_PATH}/googletest -DBENCHMARK_ENABLE_TESTING=OFF
        INSTALL_COMMAND ""
        )
add_dependencies(benchmark gtest)
add_dependencies(build-3rd benchmark)