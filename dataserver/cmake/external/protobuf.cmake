set(PROTOBUF_TAG v3.4.1)
SET(PROTOBUF_INCLUDE_DIR ${EXTERNAL_PATH}/protobuf/src)
SET(PROTOBUF_LIBRARY ${EXTERNAL_PATH}/protobuf/libprotobuf.a)
SET(PROTOBUF_PROTOC_EXE ${EXTERNAL_PATH}/protobuf/protoc)
if (NOT EXISTS ${PROTOBUF_LIBRARY} OR NOT EXISTS ${PROTOBUF_INCLUDE_DIR} OR NOT EXISTS ${PROTOBUF_PROTOC_EXE})
    ExternalProject_Add(protobuf
            PREFIX protobuf
            GIT_REPOSITORY ${PROTOBUF_URL}
            GIT_TAG ${PROTOBUF_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/protobuf
            CONFIGURE_COMMAND cmake cmake -Dprotobuf_BUILD_TESTS=OFF -DBUILD_SHARED_LIBS=OFF -Dprotobuf_BUILD_EXAMPLES=OFF
            BUILD_IN_SOURCE 1
            INSTALL_COMMAND ""
    )
    add_dependencies(build-3rd protobuf)
endif()
include_directories(${PROTOBUF_INCLUDE_DIR})
