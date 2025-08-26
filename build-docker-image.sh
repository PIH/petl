#!/bin/bash -eux

BUILD_DIR="target/docker-build"
rm -fR ${BUILD_DIR}
mkdir -p ${BUILD_DIR}

unzip m2.zip -d ${BUILD_DIR}

docker build -t partnersinhealth/petl-maven-build .