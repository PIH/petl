#!/bin/bash -eu

CONTAINER="$1"
DATABASE="$2"

# Create users if they don't exist; suppress "user already exists" error (1396) for idempotency.
# Uses CREATE USER without IF NOT EXISTS for MySQL 5.6 compatibility (IF NOT EXISTS requires 5.7.6+).
for USER in openmrs petldbadmin; do
  echo "CREATE USER '${USER}'@'%';" \
    | docker exec -i ${CONTAINER} mysql -u root -proot 2>&1 \
    | grep -v "ERROR 1396" || true
done

docker exec -i ${CONTAINER} mysql -u root -proot <<SQL
DROP DATABASE IF EXISTS ${DATABASE};
CREATE DATABASE ${DATABASE} DEFAULT CHARSET utf8;
GRANT ALL PRIVILEGES ON ${DATABASE}.* TO 'openmrs'@'%';
GRANT ALL PRIVILEGES ON ${DATABASE}.* TO 'petldbadmin'@'%';
FLUSH PRIVILEGES;
SQL

echo "$DATABASE dropped and recreated successfully in $CONTAINER container"
