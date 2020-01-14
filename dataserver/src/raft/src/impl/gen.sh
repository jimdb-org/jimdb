#!/bin/bash
protoc -I. raft.proto --cpp_out=.
