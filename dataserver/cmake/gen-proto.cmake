set(proto_FILES
    basepb/basepb.proto
    basepb/metapb.proto
    dspb/admin.proto
    dspb/api.proto
    dspb/error.proto
    dspb/expr.proto
    dspb/function.proto
    dspb/kv.proto
    dspb/processorpb.proto
    dspb/raft_internal.proto
    dspb/schedule.proto
    dspb/txn.proto
    dspb/stats.proto
    mspb/mspb.proto
)

foreach(PROTO_FILE ${proto_FILES})
    string(REPLACE .proto .pb.cc PROTO_SRC ${PROTO_FILE})
    list(APPEND proto_SOURCES proto/gen/${PROTO_SRC})
    list(APPEND proto_DEPENDS ${PROJECT_SOURCE_DIR}/../proto/${PROTO_FILE})
endforeach()

add_custom_command(OUTPUT
    ${proto_SOURCES}
    COMMAND echo "GENERATING PROTOBUF TEST SERVICE FILE on ${CMAKE_CURRENT_BINARY_DIR}"
    COMMAND echo "CURRNET SOURCE: ${PROJECT_SOURCE_DIR}"
    COMMAND cp -rf ${PROJECT_SOURCE_DIR}/../protocol proto
    COMMAND sed -i -E '/gogo/d' proto/*/*.proto
    COMMAND sed -i -E 's/github.com\\/jimdb-org\\/jimdb\\/protocol\\///' proto/*/*.proto
    COMMAND mkdir -p ./proto/gen
    COMMAND ${PROTOBUF_PROTOC_EXE} -I./proto --cpp_out=./proto/gen proto/*/*.proto
    DEPENDS build-3rd ${proto_DEPENDS}
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
set(proto_INCLUDE_DIR ${CMAKE_CURRENT_BINARY_DIR}/proto/gen ${CMAKE_CURRENT_BINARY_DIR})

