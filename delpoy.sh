#!/bin/bash
./gradlew clean build
docker cp build/libs/yarn-for-dummies-v1.jar datanode:/
docker cp build/resources/main/log4j.properties datanode:/
docker-compose exec datanode hadoop jar ./yarn-for-dummies-v1.jar .
