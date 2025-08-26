#!/bin/bash -eux

docker run --rm -v .:/data -v /var/run/docker.sock:/var/run/docker.sock partnersinhealth/petl-maven-build $@
