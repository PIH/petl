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

MAX_RETRIES="${PETL_MAX_RETRIES:-0}"
ATTEMPT=0
LOG_FILE=/tmp/petl-run.log

export SPRING_APPLICATION_JSON
SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS")

while true; do
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
