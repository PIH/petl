#!/bin/bash
set -e

if [ "$1" = '/opt/mssql/bin/sqlservr' ]; then
  if [ ! -f /tmp/db-initialized ]; then
    function create_initial_database() {
      sleep 5s
      /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "${SA_PASSWORD}" -d master -Q "CREATE DATABASE ${DATABASE_NAME}"
      touch /tmp/db-initialized
    }
    create_initial_database &
  fi
fi

exec "$@"