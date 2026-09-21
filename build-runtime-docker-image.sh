#!/bin/bash -eux

mvn clean package -DskipTests
docker build -f Dockerfile.runtime -t partnersinhealth/petl:local .
