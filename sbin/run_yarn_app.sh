#!/bin/bash
source "$(pwd)/sbin/common.sh"

load_classpath "$PWD/libs"
load_classpath "$PWD/$PROJECT_NAME/libs"
echo "classpath: $CLASSPATH"

jar=$(find "$PROJECT_NAME/build/libs" | grep jar | head -1)
echo "java -jar $jar"
java -jar "$jar"
