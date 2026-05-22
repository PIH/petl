# ETL Test Tools

This directory contains tools for testing PIH ETL pipelines (sl-etl, zl-etl, malawi-etl, etc.) against different source database configurations (MySQL versions, MariaDB) and target databases (SQL Server).

## Overview

`run-test.sh` is a wrapper script that:

1. Validates all inputs and preconditions before starting anything
2. Starts a source database container, imports a SQL dump into it
3. Starts a target database container
4. Builds the ETL project from source and runs PETL against both containers
5. Saves the run log to `logs/` for comparison across runs

The script is **not meant to be run from this directory**. Copy it (along with `containers/`) into a working directory where you keep `petl.jar`, `application.yml`, and your logs.

## Setting Up a Working Directory

Create a directory for your test environment:

```
my-work-dir/
  run-test.sh           # copied from tools/
  containers/           # copied from tools/containers/
  petl.jar              # download or build from the PETL project
  application.yml       # PETL datasource configuration
  logs/                 # created once, persists across runs
```

Copy the files:

```bash
mkdir -p ~/environments/sl-etl/logs
cp tools/run-test.sh ~/environments/sl-etl/
cp -r tools/containers ~/environments/sl-etl/
```

Then place `petl.jar` and `application.yml` in the working directory. The `application.yml` must configure datasource connections matching the compose files (source on port `3308`, SQL Server target on `1433`).

## Prerequisites

The following must be installed and available on your PATH:

- `docker` (with the Compose plugin)
- `java`
- `mvn` (Maven)
- `git`
- `pv` (pipe viewer — used to show import progress; install with `sudo apt install pv` or `brew install pv`)

The ETL source project(s) you intend to test must be cloned locally. By default the script looks under `~/code/github/pih/`, but this is configurable via `--etl-base-dir`.

A Docker network named `openmrs` must exist. Create it once if it doesn't:

```bash
docker network create openmrs
```

## Running a Test

From your working directory:

```bash
./run-test.sh \
  --db-type <type> \
  --db-version <version> \
  --branch <etl-branch> \
  --dump <path-to-sql-dump> \
  [--etl-base-dir <path>] \
  [--etl-project <name>] \
  [--target-type <type>] \
  [--target-version <version>] \
  [--database <name>] \
  [--log <log-filename>] \
  [--teardown]
```

| Argument | Required | Default | Description |
|---|---|---|---|
| `--db-type` | Yes | | Source DB type (e.g. `mysql`, `mariadb`) |
| `--db-version` | Yes | | Source DB version (e.g. `56`, `84`, `118`) |
| `--branch` | Yes | | Branch of the ETL project to build and run |
| `--dump` | Yes | | Path to the `.sql` dump file to import |
| `--etl-base-dir` | No | `~/code/github/pih` | Base directory containing ETL project checkouts |
| `--etl-project` | No | `sl-etl` | ETL project directory name within `--etl-base-dir` |
| `--target-type` | No | `sqlserver` | Target DB type |
| `--target-version` | No | `2019` | Target DB version |
| `--database` | No | `kgh` | Source database name to create and import into |
| `--log` | No | `{type}{version}-{branch}.log` | Output log filename under `logs/` |
| `--teardown` | No | | Stop and remove containers after the run |

### Examples

```bash
# MySQL 5.6, master branch (sl-etl default)
./run-test.sh --db-type mysql --db-version 56 --branch master --dump kgh-2026-05-17.sql

# MySQL 8.4, master branch
./run-test.sh --db-type mysql --db-version 84 --branch master --dump kgh-2026-05-17.sql

# MariaDB 11.8, mysql-upgrade branch, tear down when done
./run-test.sh --db-type mariadb --db-version 118 --branch mysql-upgrade --dump kgh-2026-05-17.sql --teardown

# Different ETL project
./run-test.sh --etl-project zl-etl --db-type mysql --db-version 84 --branch master --dump zl-2026-05-17.sql

# ETL project checked out outside the default base directory
./run-test.sh --etl-base-dir /work/projects --etl-project malawi-etl --db-type mysql --db-version 84 --branch master --dump malawi-2026-05-17.sql
```

The script validates all inputs and preconditions before starting any containers or long-running commands. If anything is misconfigured it reports all errors at once and exits without making changes.

## Monitoring

While a run is in progress, watch for non-succeeded jobs in a separate terminal:

```bash
watch grep -v "SUCCEEDED" logs/petl-status.log
```

## Teardown

If containers were not torn down automatically (i.e. `--teardown` was not passed), remove them manually when done:

```bash
docker compose -p sl-etl-mysql84 -f containers/mysql_84.yml down -v
docker compose -p sl-etl-sqlserver2019 -f containers/sqlserver_2019.yml down -v
```

Adjust the project name and compose file to match whichever variant was run.

## Available DB Variants

| File | Description |
|---|---|
| `containers/mysql_56.yml` | MySQL 5.6 — 2G buffer pool |
| `containers/mysql_84.yml` | MySQL 8.4 — 2G buffer pool |
| `containers/mysql_84tuned.yml` | MySQL 8.4 — 6G buffer pool, extra tuning |
| `containers/mariadb_118.yml` | MariaDB 11.8 — 2G buffer pool |
| `containers/mariadb_118tuned.yml` | MariaDB 11.8 — 6G buffer pool, query cache 256M (**recommended**) |
| `containers/mariadb_118aria.yml` | MariaDB 11.8 — same as tuned + Aria temp tables (not recommended; defeats query cache) |
| `containers/sqlserver_2019.yml` | SQL Server 2019 (default target) |

## Adding a New DB Variant

To add a new source DB variant (e.g. MySQL 9.1):

1. Add a compose file at `containers/mysql_91.yml` following the pattern of the existing MySQL files
2. If it is a new DB type with different CLI tooling, add `recreate-db.sh` and `import-db.sh` scripts under `containers/<type>/`

No changes to `run-test.sh` are needed.

To add a new target DB variant, add a compose file at `containers/<type>_<version>.yml` and pass `--target-type` and `--target-version` accordingly.
