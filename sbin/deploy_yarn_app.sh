#!/bin/bash
source "$(pwd)/sbin/common.sh"

jar=$(find "$PROJECT_NAME/build/libs" | grep jar | head -1)
log=$(find "$PROJECT_NAME/build/resources" | grep log4j | head -1)
run=$(basename "$jar")

docker cp "$jar" datanode:/
docker cp "$log" datanode:/
docker-compose exec datanode hadoop jar "./$run" .
