#!/bin/bash -eux

LOCAL_MAVEN_REPO_DIR="/tmp/petl-local-maven-repo"
mkdir -p ${LOCAL_MAVEN_REPO_DIR}
unzip m2.zip -d ${LOCAL_MAVEN_REPO_DIR}