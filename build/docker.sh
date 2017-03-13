#!/bin/bash

echo "building..."
docker build -t supernifty/burgene:latest .

version=`cat VERSION`
echo "tagging...$version"

docker tag supernifty/burgene:latest supernifty/burgene:$version

echo "pushing..."
docker push supernifty/burgene:latest
docker push supernifty/burgene:$version
