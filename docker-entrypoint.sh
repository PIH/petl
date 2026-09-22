#!/bin/sh
set -eu

JOBS="$*"
if [ -z "$JOBS" ]; then
    JOBS="${PETL_FULL_REFRESH_JOBS:-}"
fi

# $1: space-separated job names. $2: "true" to add executeLatestIncompleteJobsOnly, which
# tells petl to skip re-running any job (or job-pipeline/iterating-job child job) that
# already succeeded on a prior attempt, per its own persisted job-execution history --
# mirrors the legacy Puppet resubmit-latest-failed-job.sh.erb behavior.
build_spring_application_json() {
    incomplete_only="${2:-false}"
    json='{"petl":{"startup":{"exitAutomatically":"true"'
    if [ "$incomplete_only" = "true" ]; then
        json="${json},\"executeLatestIncompleteJobsOnly\":\"true\""
    fi
    json="${json},\"jobs\":["
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

# The first attempt runs the full job list. Every retry after that passes
# executeLatestIncompleteJobsOnly, so petl skips whatever already succeeded (per its
# own persisted job-execution history at $PETL_HOME/data) and only re-runs what
# didn't -- see build_spring_application_json above.
while true; do
    bootstrap_petl_mysql_user

    ATTEMPT=$((ATTEMPT + 1))
    if [ "$ATTEMPT" -eq 1 ]; then
        SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS" false)
    else
        SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS" true)
    fi
    export SPRING_APPLICATION_JSON

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
