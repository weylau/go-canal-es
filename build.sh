#!/bin/bash
docker build --no-cache -t registry.cn-shenzhen.aliyuncs.com/weylau/go-canal-es:latest -f Dockerfile .
docker push registry.cn-shenzhen.aliyuncs.com/weylau/go-canal-es:latest
