#!/bin/bash
set -e

gopath_array=($(go env GOPATH|tr ":" "\n"))

first_gopath=${gopath_array[0]}

# check protoc exist
command -v protoc >/dev/null 2>&1 || { echo >&2 "ERR: protoc is required but it's not installed.  Aborting."; exit 1; }

# find protoc-gen-gofast
GOGO_GOPATH="."
for path in $(echo "${GOPATH}" | sed -e 's/:/ /g'); do
    gogo_proto_bin="${path}/bin/protoc-gen-gofast"
    GOGO_GOPATH=$GOGO_GOPATH:${path}/src
    if [ -e "${gogo_proto_bin}" ]; then
        export PATH=$(dirname "${gogo_proto_bin}"):$PATH
        break
    fi
done

echo $GOGO_GOPATH


# protoc-gen-gofast not found
if [[ -z ${GOGO_GOPATH} ]]; then
    echo >&2 "ERR: Could not find protoc-gen-gofast"
    echo >&2 "Please run \`go get github.com/gogo/protobuf/protoc-gen-gofast\` first"
    exit 1;
fi

echo "gogo found in gopath: "${GOGO_GOPATH}

files=("basepb/basepb.proto" "dspb/admin.proto" "dspb/error.proto" "dspb/expr.proto" "dspb/function.proto" "dspb/kv.proto" "dspb/schedule.proto" "mspb/mspb.proto" "dspb/processorpb.proto" "dspb/api.proto" "dspb/txn.proto")

echo "generate go code..."
for file in ${files[@]}; do
    echo "process $file "
    base_name=$(basename $file ".proto")
    pkg_name=$(dirname $file)
    out_dir="../master/entity/pkg"
    mkdir -p ${out_dir}

    out_file=${out_dir}"/${pkg_name}/"${base_name}".pb.go"

    protoc -I${GOGO_GOPATH} --gofast_out=plugins=grpc:${out_dir} $file
    sed 's/github.com\/chubaodb\/chubaodb\/proto\//github.com\/chubaodb\/chubaodb\/master\/entity\/pkg\//' ${out_file} > tmpfile
    mv tmpfile ${out_file}
done
