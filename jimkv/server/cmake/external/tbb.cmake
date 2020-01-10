# tbb
set(TBB_TAG 2019_U3)
set(TBB_INCLUDE_DIR ${EXTERNAL_PATH}/tbb/include/)
set(TBB_LIBRARY ${EXTERNAL_PATH}/tbb/build/libtbb.a)
if (NOT EXISTS ${TBB_LIBRARY} OR NOT EXISTS ${TBB_INCLUDE_DIR})
    ExternalProject_Add(tbb
            PREFIX tbb
            GIT_REPOSITORY ${TBB_URL}
            GIT_TAG ${TBB_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/tbb
            CONFIGURE_COMMAND ""
            BUILD_IN_SOURCE 1
            BUILD_COMMAND sh -c "make -j $(nproc) extra_inc=big_iron.inc"
            BUILD_BYPRODUCTS ${TBB_LIBRARY}
            INSTALL_COMMAND sh -c "cp -rf ${EXTERNAL_PATH}/tbb/build/*_release/libtbb.a ${TBB_LIBRARY}"
            )
    add_dependencies(build-3rd tbb)
endif()
include_directories(${TBB_INCLUDE_DIR})