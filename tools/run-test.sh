#!/bin/bash -eu

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

ETL_BASE_DIR="$HOME/code/github/pih"
ETL_PROJECT="sl-etl"
DB_TYPE=""
DB_VERSION=""
TARGET_TYPE="sqlserver"
TARGET_VERSION="2019"
BRANCH=""
DUMP_FILE=""
DATABASE="kgh"
LOG_NAME=""
TEARDOWN=false

usage() {
  echo "Usage: $0 --db-type <type> --db-version <version> --branch <branch> --dump <file> [--etl-base-dir <path>] [--etl-project <name>] [--target-type <type>] [--target-version <version>] [--database <name>] [--log <name>] [--teardown]"
  echo "  e.g. $0 --db-type mysql --db-version 84 --branch master --dump kgh-2026-05-17.sql"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --etl-base-dir)  ETL_BASE_DIR="$2";  shift 2 ;;
    --etl-project)   ETL_PROJECT="$2";   shift 2 ;;
    --db-type)       DB_TYPE="$2";       shift 2 ;;
    --db-version)    DB_VERSION="$2";    shift 2 ;;
    --target-type)   TARGET_TYPE="$2";   shift 2 ;;
    --target-version) TARGET_VERSION="$2"; shift 2 ;;
    --branch)        BRANCH="$2";        shift 2 ;;
    --dump)          DUMP_FILE="$2";     shift 2 ;;
    --database)      DATABASE="$2";      shift 2 ;;
    --log)           LOG_NAME="$2";      shift 2 ;;
    --teardown)      TEARDOWN=true;      shift   ;;
    *) echo "Unknown argument: $1"; usage ;;
  esac
done

[[ -z "$DB_TYPE" || -z "$DB_VERSION" || -z "$BRANCH" || -z "$DUMP_FILE" ]] && usage

ETL_DIR="$ETL_BASE_DIR/$ETL_PROJECT"

SOURCE_COMPOSE="$SCRIPT_DIR/containers/${DB_TYPE}_${DB_VERSION}.yml"
TARGET_COMPOSE="$SCRIPT_DIR/containers/${TARGET_TYPE}_${TARGET_VERSION}.yml"
DB_SCRIPTS="$SCRIPT_DIR/containers/${DB_TYPE}"
SOURCE_PROJECT="${ETL_PROJECT}-${DB_TYPE}${DB_VERSION}"
TARGET_PROJECT="${ETL_PROJECT}-${TARGET_TYPE}${TARGET_VERSION}"

if [[ -z "$LOG_NAME" ]]; then
  LOG_NAME="${DB_TYPE}${DB_VERSION}-${BRANCH}.log"
fi

# --- Validation ---

ERRORS=0
error() { echo "Error: $1"; ERRORS=$((ERRORS + 1)); }

# Required commands
for CMD in docker mvn java pv git; do
  command -v "$CMD" &>/dev/null || error "Required command not found: $CMD"
done

# Docker daemon accessible
docker info &>/dev/null || error "Docker daemon is not accessible"

# Source compose file and scripts directory
[[ -f "$SOURCE_COMPOSE" ]] || error "No source compose file found: $SOURCE_COMPOSE"
[[ -d "$DB_SCRIPTS"     ]] || error "No scripts directory found: $DB_SCRIPTS"

# Target compose file
[[ -f "$TARGET_COMPOSE" ]] || error "No target compose file found: $TARGET_COMPOSE"

# Derive container names and host ports from compose service names
if [[ -f "$SOURCE_COMPOSE" ]]; then
  SOURCE_SERVICE=$(docker compose -f "$SOURCE_COMPOSE" config --services 2>/dev/null | head -1)
  [[ -n "$SOURCE_SERVICE" ]] || error "Could not determine service name from $SOURCE_COMPOSE"
  SOURCE_PORT=$(docker compose -f "$SOURCE_COMPOSE" config 2>/dev/null | grep 'published:' | head -1 | awk '{print $2}' | tr -d '"')
fi
if [[ -f "$TARGET_COMPOSE" ]]; then
  TARGET_SERVICE=$(docker compose -f "$TARGET_COMPOSE" config --services 2>/dev/null | head -1)
  [[ -n "$TARGET_SERVICE" ]] || error "Could not determine service name from $TARGET_COMPOSE"
  TARGET_PORT=$(docker compose -f "$TARGET_COMPOSE" config 2>/dev/null | grep 'published:' | head -1 | awk '{print $2}' | tr -d '"')
fi

SOURCE_CONTAINER="${SOURCE_PROJECT}-${SOURCE_SERVICE:-unknown}-1"
TARGET_CONTAINER="${TARGET_PROJECT}-${TARGET_SERVICE:-unknown}-1"

# Dump file
[[ -f "$DUMP_FILE" ]] || error "Dump file not found: $DUMP_FILE"

# Working directory contents
[[ -f "$SCRIPT_DIR/petl.jar" ]] || error "petl.jar not found at $SCRIPT_DIR/petl.jar"
[[ -d "$SCRIPT_DIR/logs"     ]] || error "Logs directory not found: $SCRIPT_DIR/logs"
[[ ! -f "$SCRIPT_DIR/logs/petl-status.log" ]] || error "logs/petl-status.log already exists from a previous run; rename or remove it first"
[[ ! -f "$SCRIPT_DIR/logs/$LOG_NAME"       ]] || error "Log file already exists: $SCRIPT_DIR/logs/$LOG_NAME"

# ETL directory and branch
if [[ ! -d "$ETL_DIR" ]]; then
  error "ETL directory not found: $ETL_DIR"
else
  CURRENT_BRANCH=$(git -C "$ETL_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null) || { error "Could not determine git branch in $ETL_DIR"; CURRENT_BRANCH=""; }
  [[ "$CURRENT_BRANCH" == "$BRANCH" ]] || error "ETL project is on branch '$CURRENT_BRANCH', expected '$BRANCH'"
  if ! git -C "$ETL_DIR" diff --quiet || ! git -C "$ETL_DIR" diff --cached --quiet; then
    echo "Warning: ETL project has uncommitted changes"
  fi
fi

# Docker network
docker network inspect openmrs &>/dev/null || error "Docker network 'openmrs' does not exist"

# Containers must not already exist
docker ps -a --format '{{.Names}}' | grep -qx "$SOURCE_CONTAINER" && error "Container '$SOURCE_CONTAINER' already exists"
docker ps -a --format '{{.Names}}' | grep -qx "$TARGET_CONTAINER" && error "Container '$TARGET_CONTAINER' already exists"

# Host ports must not already be bound
[[ -n "$SOURCE_PORT" ]] && docker ps --format '{{.Ports}}' | grep -q ":${SOURCE_PORT}->" && error "Port $SOURCE_PORT is already bound by a running container"
[[ -n "$TARGET_PORT" ]] && docker ps --format '{{.Ports}}' | grep -q ":${TARGET_PORT}->" && error "Port $TARGET_PORT is already bound by a running container"

if [[ $ERRORS -gt 0 ]]; then
  echo "$ERRORS validation error(s) found. Aborting."
  exit 1
fi

wait_for_mysql() {
  local container="$1"
  local client="$2"
  local timeout=120
  local elapsed=0
  echo "Waiting for database to be ready in $container..."
  until docker exec "$container" "$client" -u root -proot -e "SELECT 1" &>/dev/null; do
    if [[ $elapsed -ge $timeout ]]; then
      echo "Error: Database in $container did not become ready within ${timeout}s"
      exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "Database is ready."
}

wait_for_sqlserver() {
  local container="$1"
  local timeout=120
  local elapsed=0
  echo "Waiting for SQL Server database to be ready in $container..."
  until docker exec "$container" bash -c \
    '/opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P "$SA_PASSWORD" -d "$DATABASE_NAME" -Q "SELECT 1"' \
    &>/dev/null; do
    if [[ $elapsed -ge $timeout ]]; then
      echo "Error: SQL Server in $container did not become ready within ${timeout}s"
      exit 1
    fi
    sleep 2
    elapsed=$((elapsed + 2))
  done
  echo "SQL Server is ready."
}

echo "Starting test run: etl=$ETL_PROJECT source=${DB_TYPE}${DB_VERSION} target=${TARGET_TYPE}${TARGET_VERSION} branch=$BRANCH database=$DATABASE log=logs/$LOG_NAME"

case "$DB_TYPE" in
  mariadb) DB_CLIENT="mariadb" ;;
  *)       DB_CLIENT="mysql"   ;;
esac

docker compose -p "$SOURCE_PROJECT" -f "$SOURCE_COMPOSE" up -d
wait_for_mysql "$SOURCE_CONTAINER" "$DB_CLIENT"
"$DB_SCRIPTS/recreate-db.sh" "$SOURCE_CONTAINER" "$DATABASE"
"$DB_SCRIPTS/import-db.sh" "$SOURCE_CONTAINER" "$DATABASE" "$DUMP_FILE"

docker compose -p "$TARGET_PROJECT" -f "$TARGET_COMPOSE" up -d
wait_for_sqlserver "$TARGET_CONTAINER"

mvn -f "$ETL_DIR" clean install
java -jar "$SCRIPT_DIR/petl.jar"

mv "$SCRIPT_DIR/logs/petl-status.log" "$SCRIPT_DIR/logs/$LOG_NAME"
echo "Log saved to $SCRIPT_DIR/logs/$LOG_NAME"

if $TEARDOWN; then
  docker compose -p "$TARGET_PROJECT" -f "$TARGET_COMPOSE" down -v
  docker compose -p "$SOURCE_PROJECT" -f "$SOURCE_COMPOSE" down -v
fi
