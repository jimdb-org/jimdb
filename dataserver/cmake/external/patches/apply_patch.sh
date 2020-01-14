#!/bin/bash
set -e

BASEDIR=`dirname $0`
OS_NAME=`uname -s | awk '{print tolower($0)}'`

PKG_NAME=$1
if [ "$PKG_NAME" == "" ];then
   echo "ERR: target package name is required."
   echo "Usage: ./apply_patch.sh PACKAGE_NAME"
   exit 1
fi

# final patch files to apply
declare -a APPLY_PATCHES=() 

for PATCH_FILE in `ls ${BASEDIR}/${PKG_NAME}/*.patch`;
do
    PATCH_OS=`basename ${PATCH_FILE} | awk -F - '{print $2}'`
    # check patch's os prefix
    if [ "$PATCH_OS" != "generic" ] &&  [ "$PATCH_OS" != "darwin" ] && [ "$PATCH_OS" != "linux" ]; then
        echo "ERR: Invalid os prefix (${PATCH_OS}) of patch file: "${PATCH_FILE}
        exit 1
    fi
    # check if os prefix match current os
    if [ "$PATCH_OS" == "generic" ] ||  [ "$PATCH_OS" == "$OS_NAME" ]; then
        APPLY_PATCHES+=(${PATCH_FILE})
    fi
done

for PATCH_FILE in "${APPLY_PATCHES[@]}";
do
    echo ${PATCH_FILE}
    patch -p 1 < ${PATCH_FILE}
done

