#!/usr/bin/env bash


echo "Compile JIMDB"
./build_image.sh

echo "Make JIMDB Image"
docker build -t ansj/jimdb:0.1 .

echo "Start a empty service"
docker run ansj/jimdb:0.1

