#!/bin/sh
set -eu

JOBS="$*"
if [ -z "$JOBS" ]; then
    JOBS="${PETL_FULL_REFRESH_JOBS:-}"
fi

build_spring_application_json() {
    json='{"petl":{"startup":{"exitAutomatically":"true","jobs":['
    sep=""
    for job in $1; do
        json="${json}${sep}\"${job}\""
        sep=", "
    done
    json="${json}]}}}"
    echo "$json"
}

bootstrap_petl_mysql_user() {
    if [ -z "${PETL_MYSQL_ROOT_PASSWORD:-}" ]; then
        return 0
    fi
    echo "Bootstrapping PETL MySQL user '${PETL_MYSQL_USER}'..."
    mysql -h "${PETL_MYSQL_HOST}" -P "${PETL_MYSQL_PORT:-3306}" -uroot -p"${PETL_MYSQL_ROOT_PASSWORD}" <<-SQL
CREATE USER IF NOT EXISTS '${PETL_MYSQL_USER}'@'%' IDENTIFIED BY '${PETL_MYSQL_PASSWORD}';
GRANT ALL PRIVILEGES ON *.* TO '${PETL_MYSQL_USER}'@'%';
FLUSH PRIVILEGES;
SQL
}

MAX_RETRIES="${PETL_MAX_RETRIES:-0}"
ATTEMPT=0
LOG_FILE=/tmp/petl-run.log

export SPRING_APPLICATION_JSON
SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS")

while true; do
    bootstrap_petl_mysql_user

    ATTEMPT=$((ATTEMPT + 1))
    echo "Executing PETL (attempt ${ATTEMPT}) with configuration:"
    echo "$SPRING_APPLICATION_JSON"

    java -jar /home/petl/bin/petl.jar 2>&1 | tee "$LOG_FILE"

    if ! grep -q "org.pih.petl.PetlException" "$LOG_FILE"; then
        echo "PETL execution completed successfully"
        exit 0
    fi

    echo "PETL execution completed with errors (attempt ${ATTEMPT})"
    if [ "$ATTEMPT" -gt "$MAX_RETRIES" ]; then
        echo "Maximum number of attempts (${MAX_RETRIES}) reached, terminating"
        exit 1
    fi
    echo "Retrying..."
done
