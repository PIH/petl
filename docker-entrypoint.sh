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

export SPRING_APPLICATION_JSON
SPRING_APPLICATION_JSON=$(build_spring_application_json "$JOBS")

echo "Executing PETL with configuration:"
echo "$SPRING_APPLICATION_JSON"

exec java -jar /home/petl/bin/petl.jar
