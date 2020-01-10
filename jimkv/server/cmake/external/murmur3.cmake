# murmur3 library
set(MURMUR3_LIBRARY ${PROJECT_SOURCE_DIR}/third-party/murmur3/build/libmurmur3.a)
if (NOT EXISTS ${MURMUR3_LIBRARY})
  ExternalProject_Add(murmur3
    PREFIX murmur3
    SOURCE_DIR ${PROJECT_SOURCE_DIR}/third-party/murmur3
    BINARY_DIR ${PROJECT_SOURCE_DIR}/third-party/murmur3/build
    CONFIGURE_COMMAND cmake ..
    INSTALL_COMMAND ""
    )
  add_dependencies(build-3rd murmur3)
endif()
include_directories(${PROJECT_SOURCE_DIR}/third-party/murmur3)
