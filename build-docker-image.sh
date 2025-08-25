#!/bin/bash -eux

rsync -a ~/m2 ./
docker build -t partnersinhealth/petl-maven-build .
rm -fR ./m2