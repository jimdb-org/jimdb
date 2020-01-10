#!/bin/bash

# usage:
#
# build dataserver: 
#     run build_ds.sh
#
# build dataserver and run test: 
#     run build_ds.sh test
#

set -e

SOURCE_DIR=`dirname $0`/..
SOURCE_DIR=`(cd "$SOURCE_DIR"; pwd)`

# build dir
BUILD_DIR=${SOURCE_DIR}/build/ds-build
echo "build path: "${BUILD_DIR}
mkdir -p ${BUILD_DIR}
cd ${BUILD_DIR}

# check if run test
RUN_TEST=false
TEST_REPORT_FORMART="json" # json or xml
TEST_REPORT_DIR=${BUILD_DIR}"/unittest_reports/"
if [ "$#" -eq 1 ] && [ "$1" == "test" ]; then
    echo "unittest enabled, report dir: "${TEST_REPORT_DIR}
    RUN_TEST=true
fi

# configure
if [ "$RUN_TEST" = true ]; then
    cmake ${SOURCE_DIR}/dataserver -DBUILD_TEST=ON -DTEST_RUN_ARGS=--gtest_output=${TEST_REPORT_FORMART}:${TEST_REPORT_DIR}
else 
    cmake ${SOURCE_DIR}/dataserver
fi

# build
make -j `nproc`

# run tests
if [ "$RUN_TEST" = true ]; then
    rm -f ${TEST_REPORT_DIR}/*.${TEST_REPORT_FORMART}
    ctest
fi
