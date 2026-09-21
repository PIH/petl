#!/bin/bash -eux

./create-local-maven-repo.sh
mvn clean package -DskipTests -Dmaven.source.skip=true -Dmaven.javadoc.skip=true
docker build -f Dockerfile.runtime -t partnersinhealth/petl:local .
