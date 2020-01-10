#!/usr/bin/env bash


echo "Compile Chubaodb"
./build_image.sh

echo "Make Chubaodb Image"
docker build -t ansj/chubaodb:0.1 .

echo "Start a empty service"
docker run ansj/chubaodb:0.1

