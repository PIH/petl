#!/bin/bash

set -uo pipefail
IFS=$'\n\t'

function usage() {
  echo "USAGE:"
  echo "startup-mysql.sh --containerName=openmrs-mysql --mysqlDataDir=/path/to/migration/dir --mysqlPort=3307"
}

RETURN_CODE=0

CONTAINER_NAME=""
MYSQL_DATA_DIR=""
MYSQL_PORT="3307"

for i in "$@"
do
case $i in
    --containerName=*)
      CONTAINER_NAME="${i#*=}"
      shift # past argument=value
    ;;
    --mysqlDataDir=*)
      MYSQL_DATA_DIR="${i#*=}"
      shift # past argument=value
    ;;
    --mysqlPort=*)
      MYSQL_PORT="${i#*=}"
      shift # past argument=value
    ;;
    *)
      usage    # unknown option
      exit 1
    ;;
esac
done

if [ -z "$CONTAINER_NAME" ]; then
  echo "You must specify a valid container name"
  usage
  exit 1
fi

if [ ! -d $MYSQL_DATA_DIR ]; then
  echo "You must specify a valid migration dir"
  usage
  exit 1
fi

if [ -z "$MYSQL_PORT" ]; then
  echo "You must specify a valid mysql port"
  usage
  exit 1
fi

docker run --name $CONTAINER_NAME -d -p $MYSQL_PORT:3306 -v $MYSQL_DATA_DIR:/var/lib/mysql mysql:8.0 \
      --character-set-server=utf8 \
      --collation-server=utf8_general_ci \
      --max_allowed_packet=1G \
      --innodb-buffer-pool-size=1G

