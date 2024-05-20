#!/bin/bash

if [[ $# -ne 2 ]]; then
  echo "$0 [project-name] [build]"
  echo "$0 hadoop-yarn-application true"
  exit 1
fi

PROJECT_NAME=$1
BUILD=$2
PWD=$(pwd)
CLASSPATH="."

function clean_and_build() {
  ./gradlew ":$PROJECT_NAME:clean" ":$PROJECT_NAME:build"
  res=$?
  if [[ $res -ne 0 ]]; then
    echo "build failed with $res"
    exit
  fi
  echo "clean & build $PROJECT_NAME completed"
}

function load_classpath() {
  local target=$1
  for jar in "$target"/*.jar; do
      CLASSPATH="$CLASSPATH:$jar"
  done
}

if [[ $BUILD == "true" ]]; then
  clean_and_build
fi
