set(ASIO_TAG asio-1-14-0)
set(ASIO_INCLUDE_DIR ${EXTERNAL_PATH}/asio/asio/include)
if (NOT EXISTS ${ASIO_INCLUDE_DIR})
    ExternalProject_Add(asio
            PREFIX sio
            GIT_REPOSITORY ${ASIO_URL}
            GIT_TAG ${ASIO_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/asio
            CONFIGURE_COMMAND ""
            BUILD_COMMAND ""
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd asio)
endif()
include_directories(${ASIO_INCLUDE_DIR})
