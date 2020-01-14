find_package(CURL REQUIRED)
if(NOT CURL_FOUND)
    MESSAGE(FATAL_ERROR "CURL library and development files is required")
else()
    MESSAGE(STATUS "CURL is found, version: ${CURL_VERSION_STRING}")
endif()

# cpr library
set(CPR_LIBRARY ${PROJECT_SOURCE_DIR}/third-party/cpr/build/lib/libcpr.a)
if (NOT EXISTS ${CPR_LIBRARY})
    ExternalProject_Add(cpr
            PREFIX cpr
            SOURCE_DIR ${PROJECT_SOURCE_DIR}/third-party/cpr
            BINARY_DIR ${PROJECT_SOURCE_DIR}/third-party/cpr/build
            CONFIGURE_COMMAND cmake .. -DUSE_SYSTEM_CURL=ON -DBUILD_CPR_TESTS=OFF
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd cpr)
endif()
include_directories(${PROJECT_SOURCE_DIR}/third-party/cpr/include)
