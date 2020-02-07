#!/bin/bash

mkdir ../jimsql/jim-sdk/jim-proto/src/main/proto
cp -rf ../protocol/*pb ../jimsql/jim-sdk/jim-proto/src/main/proto
cd ../jimsql/jim-sdk/jim-proto/src/main || exit

OS_NAME=`uname -s | awk '{print tolower($0)}'`
#linux or mac
if [ "$OS_NAME" == "linux" ]; then
    sed -i '/gogo/d' $(grep -rl gogo proto)
    sed -i 's/github.com\/jimdb-org\/jimdb\/protocol\///' $(grep -rl github.com proto)
    sed -i "/package/a\\option java_package=\"io.jimdb.pb\";" proto/*/*.proto
    sed -i "/java_package/a\\option java_outer_classname=\"Exprpb\";" proto/dspb/expr.proto
    sed -i "/java_package/a\\option java_outer_classname=\"Errorpb\";" proto/dspb/error.proto
    sed -i "/java_package/a\\option java_outer_classname=\"Statspb\";" proto/dspb/stats.proto
else
    sed -i "" '/gogo/d' $(grep -rl gogo proto)
    sed -i "" 's/github.com\/jimdb-org\/jimdb\/protocol\///' $(grep -rl github.com proto)
    sed -i "" "/package/a\\
    option java_package=\"io.jimdb.pb\";" proto/*/*.proto
    sed -i "" "/java_package/a\\
    option java_outer_classname=\"Exprpb\";" proto/dspb/expr.proto
    sed -i "" "/java_package/a\\
    option java_outer_classname=\"Errorpb\";" proto/dspb/error.proto
    sed -i "" "/java_package/a\\
    option java_outer_classname=\"Statspb\";" proto/dspb/stats.proto
fi

cd proto || exit

protoc --java_out=../java ./*/*.proto

rm -rf ../proto
