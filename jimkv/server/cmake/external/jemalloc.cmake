SET(JEMALLOC_TAG 5.2.1)
SET(JEMALLOC_INCLUDE_DIR ${EXTERNAL_PATH}/jemalloc/include)
set(JEMALLOC_LIBRARY ${EXTERNAL_PATH}/jemalloc/lib/libjemalloc.a)
if (NOT EXISTS ${JEMALLOC_LIBRARY} OR NOT EXISTS ${JEMALLOC_INCLUDE_DIR})
    ExternalProject_Add(jemalloc
            PREFIX jemalloc
            GIT_REPOSITORY ${JEMALLOC_URL}
            GIT_TAG ${JEMALLOC_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/jemalloc
            CONFIGURE_COMMAND ./autogen.sh && ./configure --enable-prof --enable-shared=no --enable-stats
            --with-jemalloc-prefix=""
            BUILD_IN_SOURCE true
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd jemalloc)
endif()
include_directories(${JEMALLOC_INCLUDE_DIR})