#!/bin/bash

# This takes a mysql 5.6 dump file and

# Required libraries:  docker, pv, maven, java

set -uo pipefail
IFS=$'\n\t'

function usage() {
  echo "USAGE:"
  echo "execute-2x-migration.sh --inputSqlFile=/path/to/sql/file.sql --targetMysqlDataDir=/path/to/migration/dir --mysqlPort=3307 --mysqlRootPassword=root"
}

RETURN_CODE=0

INPUT_SQL_FILE=""
MYSQL_ROOT_PASSWORD="root"
TARGET_MYSQL_DATA_DIR=""
MYSQL_PORT="3307"

for i in "$@"
do
case $i in
    --inputSqlFile=*)
      INPUT_SQL_FILE="${i#*=}"
      shift # past argument=value
    ;;
    --targetMysqlDataDir=*)
      MYSQL_DATA_DIR="${i#*=}"
      shift # past argument=value
    ;;
      --mysqlPort=*)
        MYSQL_PORT="${i#*=}"
        shift # past argument=value
    ;;
      --mysqlRootPassword=*)
        MYSQL_ROOT_PASSWORD="${i#*=}"
        shift # past argument=value
    ;;
    *)
      usage    # unknown option
      exit 1
    ;;
esac
done

if [ ! -f $INPUT_SQL_FILE ]; then
  echo "You must specify a valid input SQL file"
  usage
  exit 1
fi

if [ ! -d $MYSQL_DATA_DIR ]; then
  echo "You must specify a valid migration dir"
  usage
  exit 1
fi

MYSQL_CONTAINER="openmrs-mysql8-upgrade"

mkdir -p $MYSQL_DATA_DIR

function start_mysql_container() {
  MYSQL_VERSION=${1:-5.6}
  echo "$(date): Creating a MySQL $MYSQL_VERSION container"
  docker run --name $MYSQL_CONTAINER -d -p $MYSQL_PORT:3306 \
    -v $MYSQL_DATA_DIR:/var/lib/mysql \
    -e MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD \
    -e MYSQL_USER=openmrs \
    -e MYSQL_PASSWORD=openmrs \
    -e MYSQL_DATABASE=openmrs \
    mysql:$MYSQL_VERSION \
      --character-set-server=utf8 \
      --collation-server=utf8_general_ci \
      --max_allowed_packet=1G \
      --innodb-buffer-pool-size=1G

  echo "$(date): Container created. Waiting 10 seconds..."
  sleep 10
}

function remove_mysql_container() {
  echo "$(date): Stopping and removing MySQL container"
  docker stop $MYSQL_CONTAINER || true
  docker rm $MYSQL_CONTAINER || true
  echo "$(date): MySQL container removed"
}

# Import the starting sql file into the mysql container.  This uses pv to monitor progress, as this can take over an hour
function import_initial_db() {
  echo "$(date): Importing the provided database backup"
  pv $INPUT_SQL_FILE | docker exec -i $MYSQL_CONTAINER sh -c 'exec mysql -u root -proot openmrs'
  echo "$(date): Import complete"
}

function run_mysql_upgrade() {
  docker exec -it $MYSQL_CONTAINER mysql_upgrade -uroot -proot
}

function execute_all() {

  remove_mysql_container

  # Create a docker mysql 5.6 container to use for the initial upgrade and import initial sql file
  start_mysql_container "5.6"
  import_initial_db

  # Upgrade to MySQL 5.7
  remove_mysql_container
  start_mysql_container "5.7"
  run_mysql_upgrade

  # Upgrade to MySQL 5.8
  remove_mysql_container
  start_mysql_container "8.0"
  run_mysql_upgrade

  # Clean up
  remove_mysql_container
}

execute_all
