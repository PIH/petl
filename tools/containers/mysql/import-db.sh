#!/bin/bash -eu

CONTAINER="$1"
DATABASE="$2"
IMPORT_FILE="$3"

echo "Importing into ${DATABASE} in ${CONTAINER} container from ${IMPORT_FILE}"
pv ${IMPORT_FILE} | docker exec -i ${CONTAINER} sh -c "exec mysql -u root -proot ${DATABASE}"

