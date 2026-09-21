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

    # MYSQL_PWD rather than -p on the command line: container process arguments
    # show up in the host's ps output, the environment does not.
    EXISTING_USER_COUNT=$(MYSQL_PWD="${PETL_MYSQL_ROOT_PASSWORD}" mysql -h "${PETL_MYSQL_HOST}" -P "${PETL_MYSQL_PORT:-3306}" -uroot -N -e \
        "SELECT COUNT(*) FROM mysql.user WHERE user = '${PETL_MYSQL_USER}' AND host = '%';")

    if [ "${EXISTING_USER_COUNT}" -eq 0 ]; then
        echo "PETL MySQL user '${PETL_MYSQL_USER}' not found, creating"
        # CREATE USER statement contains a password, so it's passed via stdin
        # rather than -e, which would otherwise expose it in argv too.
        MYSQL_PWD="${PETL_MYSQL_ROOT_PASSWORD}" mysql -h "${PETL_MYSQL_HOST}" -P "${PETL_MYSQL_PORT:-3306}" -uroot <<-EOSQL
			CREATE USER '${PETL_MYSQL_USER}'@'%' IDENTIFIED BY '${PETL_MYSQL_PASSWORD}';
		EOSQL
    else
        echo "PETL MySQL user '${PETL_MYSQL_USER}' already exists, not re-creating"
    fi

    MYSQL_PWD="${PETL_MYSQL_ROOT_PASSWORD}" mysql -h "${PETL_MYSQL_HOST}" -P "${PETL_MYSQL_PORT:-3306}" -uroot <<-EOSQL
		GRANT ALL PRIVILEGES ON *.* TO '${PETL_MYSQL_USER}'@'%';
		FLUSH PRIVILEGES;
	EOSQL
}

MAX_RETRIES="${PETL_MAX_RETRIES:-0}"
case "$MAX_RETRIES" in
    ''|*[!0-9]*) MAX_RETRIES=0 ;;
esac
ATTEMPT=0
LOG_FILE=/tmp/petl-run.log

export SPRING_APPLICATION_JSON
SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS")

# Note: this retries by re-running the FULL job list on every attempt. The
# legacy Puppet retry mechanism (resubmit-latest-failed-job.sh.erb) only
# re-ran jobs that hadn't completed, via an executeLatestIncompleteJobsOnly
# flag. We intentionally don't replicate that here: partial retry requires
# understanding petl's own job-tracking semantics in depth, and a full
# re-run is still correct, just potentially slower.
while true; do
    bootstrap_petl_mysql_user

    ATTEMPT=$((ATTEMPT + 1))
    echo "Executing PETL (attempt ${ATTEMPT}) with configuration:"
    echo "$SPRING_APPLICATION_JSON"

    { java -jar /home/petl/bin/petl.jar 2>&1 || echo "PETL_JAVA_EXIT_NONZERO"; } | tee "$LOG_FILE"

    if ! grep -qE "org\.pih\.petl\.PetlException|PETL_JAVA_EXIT_NONZERO" "$LOG_FILE"; then
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
