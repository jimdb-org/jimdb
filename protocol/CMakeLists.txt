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
    list(APPEND proto_SOURCES ${CMAKE_CURRENT_BINARY_DIR}/gen/${PROTO_SRC})
    list(APPEND proto_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/${PROTO_FILE})
endforeach()

include_directories(${PROTOBUF_INCLUDE_DIR})

add_custom_command(OUTPUT
    ${proto_SOURCES}
    COMMAND echo "GENERATING PROTOBUF CPP FILES on ${CMAKE_CURRENT_BINARY_DIR}"
    COMMAND echo "CURRNET SOURCE: ${CMAKE_CURRENT_SOURCE_DIR}"
    COMMAND mkdir -p proto
    COMMAND cp -rf ${CMAKE_CURRENT_SOURCE_DIR}/*pb proto/
    COMMAND sed -i -E '/gogo/d' proto/*/*.proto
    COMMAND sed -i -E 's/github.com\\/jimdb-org\\/jimdb\\/protocol\\///' proto/*/*.proto
    COMMAND mkdir -p ./gen
    COMMAND ${PROTOBUF_PROTOC_EXE} -I./proto --cpp_out=./gen proto/*/*.proto
    DEPENDS ${proto_DEPENDS} build-3rd
    WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/gen)
add_library(jim-proto STATIC ${proto_SOURCES})
