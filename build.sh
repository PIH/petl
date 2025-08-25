#!/bin/bash -eux

MAVEN_ARGS="${1:-clean verify}"

docker run --rm -v .:/data -v /var/run/docker.sock:/var/run/docker.sock partnersinhealth/petl-maven-build ${MAVEN_ARGS}
