#!/bin/bash

set -e
export JAVA_HOME=${GRAALVM_HOME}

echo "> building jar"

./gradlew clean shadow
mkdir -p build/graal

echo "> compiling binary"
${GRAALVM_HOME}/bin/native-image \
  -H:Path=./build/graal \
  -H:+ReportExceptionStackTraces \
  -cp ./build/libs/banktivity-stock-quote-sync-kotlin-all.jar
