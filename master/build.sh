#!/usr/bin/env bash
# version value
BUILD_VERSION="0.2"

flags="-X 'main.BuildVersion=$BUILD_VERSION' -X 'main.CommitID=$(git rev-parse HEAD)' -X 'main.BuildTime=$(date +"%Y-%m-%d %H:%M.%S")'"
echo "version info: $flags"

go build -ldflags "$flags" -o master cmd/startup.go
