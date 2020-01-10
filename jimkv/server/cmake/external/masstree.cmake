SET(MASSTREE_INCLUDE_DIR ${EXTERNAL_PATH})
SET(MASSTREE_LIBRARY ${EXTERNAL_PATH}/masstree-beta/libmass-tree.a)
SET(MASSTREE_TAG "cfc62fa6d0f3b7c6d023bba529bed8f8456a674a")
if (NOT EXISTS ${MASSTREE_LIBRARY})
    ExternalProject_Add(masstree
            PREFIX masstree
            GIT_REPOSITORY ${MASSTREE_URL}
            GIT_TAG ${MASSTREE_TAG}
            SOURCE_DIR ${EXTERNAL_PATH}/masstree-beta
            PATCH_COMMAND ${CMAKE_CURRENT_LIST_DIR}/patches/apply_patch.sh masstree
            CONFIGURE_COMMAND
                ./bootstrap.sh
                && env CXXFLAGS=-std=c++11 ./configure --disable-superpage --disable-preconditions --enable-max-key-len=1048576
                && cmake .
            BUILD_COMMAND sh -c "make -f Makefile -j $(nproc)"
            BUILD_IN_SOURCE true
            INSTALL_COMMAND ""
            )
    add_dependencies(build-3rd masstree)
endif()
include_directories(${MASSTREE_INCLUDE_DIR})