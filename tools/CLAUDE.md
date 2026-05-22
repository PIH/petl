# ETL Test Tools — Development Notes

This file captures conventions and architectural decisions for the `tools/` directory in the petl project.

## What This Is

A portable test harness for PIH ETL pipelines (sl-etl, zl-etl, malawi-etl, etc.). It spins up source and target database containers, imports a SQL dump, builds and runs the PETL pipeline, and saves the result log for comparison across runs.

`run-test.sh` is intended to be **copied** into a working directory alongside `petl.jar`, `application.yml`, and the `containers/` directory. The `containers/` directory here serves as the canonical reference set of configurations.

## How `run-test.sh` Discovers Things

The script is pattern-driven, not hardcoded to specific DB variants. Given `--db-type mysql` and `--db-version 84` it expects:

- **Compose file**: `containers/mysql_84.yml` (relative to the script's location)
- **Scripts directory**: `containers/mysql/`
- **ETL dir**: `{etl-base-dir}/{etl-project}` (defaults: `~/code/github/pih` and `sl-etl`)
- **Project name**: `{etl-project}-{db-type}{db-version}` e.g. `sl-etl-mysql84` (passed to `docker compose -p`)
- **Container name**: `{project}-{service}-1` where `{service}` is read dynamically from the compose file via `docker compose config --services`
- **Naming convention**: hyphens are used consistently throughout — project names, container names, log filenames. No conversion between `-` and `_` is needed or performed.

The service name in the compose file matters — it becomes part of the auto-generated container name. The script reads it at runtime so it doesn't need to be hardcoded.

## Working Directory Layout

When a user sets up their working directory they should have:

```
run-test.sh           # copied from tools/
containers/           # copied from tools/containers/ (or a subset)
petl.jar
application.yml
logs/
```

The script resolves all paths from `$SCRIPT_DIR` (the directory containing the script), so everything is relative to where it lives.

## Adding a New Source DB Variant

### Same type, new version (e.g. MySQL 9.1)

1. Create `containers/mysql_91.yml` based on an existing MySQL compose file
2. Adjust the `image:` tag and `command:` flags for the new version
3. That's it — `run-test.sh` will find it automatically

### New type entirely (e.g. Percona)

1. Create `containers/percona_84.yml` with a service name of your choice
2. Create `containers/percona/recreate-db.sh` and `containers/percona/import-db.sh`
   - Scripts differ per type because they call the DB CLI tool by name (`mysql` vs `mariadb`)
   - `recreate-db.sh` creates users and the database; see existing scripts for the pattern
   - `import-db.sh` pipes the dump via `pv` into the container
3. Pass `--db-type percona --db-version 84` to `run-test.sh`

## Compose File Conventions

- Omit `name:` and `container_name:` — project name is passed via `docker compose -p` at runtime
- Source DBs all map to port `3308:3306` on the host; only one can run at a time
- All compose files attach to the external `openmrs` Docker network (must be created once: `docker network create openmrs`)
- The SQL Server target maps to `1433:1433`

## Known Quirks

- `containers/mariadb_118.yml` uses `mysql` as the service name (not `mariadb`). This is a holdover — it means the container is named e.g. `sl-etl-mariadb118-mysql-1`. Be consistent if adding new mariadb versions.
- Source DB root credentials (`root`/`root`) are set in two places: the compose file `environment` block and hardcoded as `-proot` in the `recreate-db.sh` and `import-db.sh` scripts. If you change one, change both.
- `application.yml` connection config must stay in sync with the compose files. Key values: source port `3308`, target port `1433`, target password matches `SA_PASSWORD` in `containers/sqlserver_2019/Dockerfile`.
- The `recreate-db.sh` scripts use `CREATE USER` without `IF NOT EXISTS` for MySQL 5.6 compatibility (that syntax requires 5.7.6+). They suppress the "user already exists" error (1396) instead.

## Target DB Variants

The same pattern applies to target DBs. To add SQL Server 2022:

1. Create `containers/sqlserver_2022/Dockerfile` and `entrypoint.sh` based on the 2019 versions
2. Create `containers/sqlserver_2022.yml`
3. Pass `--target-version 2022` to `run-test.sh`
