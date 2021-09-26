#!/bin/bash
path="."
for jar in `ls build/libs`; do
    path="$path:build/libs/$jar"
done
# set CLASSPATH $path

classpath="-cp .:./build/libs/*"
jars="-jar build/libs/yarn-for-dummies-1.0-SNAPSHOT.jar"

echo "java $classpath $jars"
java $classpath $jars
