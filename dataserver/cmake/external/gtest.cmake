# gtest
set(GTEST_TAG release-1.8.0)
set(GTEST_INCLUDE_DIR ${EXTERNAL_PATH}/googletest/googletest/include)
set(GMOCK_INCLUDE_DIR ${EXTERNAL_PATH}/googletest/googlemock/include)
set(GTEST_LIBRARY ${EXTERNAL_PATH}/googletest/lib/libgtest.a)
ExternalProject_Add(gtest
        PREFIX gtest
        GIT_REPOSITORY ${GTEST_URL}
        GIT_TAG ${GTEST_TAG}
        SOURCE_DIR ${EXTERNAL_PATH}/googletest
        BUILD_IN_SOURCE 1
        CONFIGURE_COMMAND cmake .
        INSTALL_COMMAND ""
        )
add_dependencies(build-3rd gtest)