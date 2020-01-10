set(ROCKSDB_TAG v6.4.6)
set(ROCKSDB_INCLUDE_DIR ${EXTERNAL_PATH}/rocksdb/include)
set(ROCKSDB_LIBRARY ${EXTERNAL_PATH}/rocksdb/librocksdb.a)
if (NOT EXISTS ${ROCKSDB_LIBRARY} OR NOT EXISTS ${ROCKSDB_INCLUDE_DIR})
    ExternalProject_Add(rocksdb
            PREFIX rocksdb
            GIT_REPOSITORY ${ROCKSDB_URL}
            GIT_TAG ${ROCKSDB_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/rocksdb
            PATCH_COMMAND ${CMAKE_CURRENT_LIST_DIR}/patches/apply_patch.sh rocksdb
            CONFIGURE_COMMAND cmake . -DWITH_TESTS=OFF -DPORTABLE=ON -DWITH_TOOLS=OFF
            BUILD_IN_SOURCE 1
            BUILD_BYPRODUCTS ${ROCKSDB_LIBRARY}
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd rocksdb)
endif()
include_directories(${ROCKSDB_INCLUDE_DIR})
