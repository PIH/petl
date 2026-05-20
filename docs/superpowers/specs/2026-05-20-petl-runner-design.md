# petl-runner Design Spec

**Date:** 2026-05-20
**Status:** Draft

## Overview

`petl-runner` is a Python CLI tool that provides a managed test and development harness for PETL-based ETL pipelines. It handles the full lifecycle of a test run: provisioning source and target database containers, building the ETL project, running PETL, capturing structured results, and supporting comparison across runs. It is ETL-project-agnostic and reusable across any PETL-based pipeline (sl-etl, malawi-etl, etc.).

---

## Location

`tools/petl-runner/` in the PETL project (version controlled). All runtime data lives in a separate working directory outside version control.

```
tools/petl-runner/
├── petl_runner/
│   ├── cli.py          # click entry points and argument parsing
│   ├── docker.py       # container lifecycle
│   ├── runner.py       # run orchestration
│   ├── log_parser.py   # PETL log → job tree
│   ├── results.py      # result storage and loading
│   └── compare.py      # diff table generation
└── pyproject.toml
```

Dependencies: `click`, `rich`.

---

## Working Directory

The working directory (e.g. `~/environments/petl-runner/`) is specified via `--work-dir` or the `PETL_RUNNER_HOME` environment variable. It holds all runtime data and is never checked into version control.

```
~/environments/petl-runner/
├── petl-runner.yml              # working directory config (default jar, etc.)
├── petl/                        # petl jar(s)
│   └── petl-3.8.0-SNAPSHOT.jar
├── sources/                     # source DB docker-compose configs
│   ├── mysql56/docker-compose.yml
│   ├── mysql84/docker-compose.yml
│   └── mariadb118/docker-compose.yml
├── targets/                     # target DB docker-compose configs
│   └── sqlserver/docker-compose.yml
├── dumps/                       # SQL dump files
├── matrices/                    # matrix run definitions
├── data/                        # DB data directories
│   ├── mysql56_<compose+dump-hash>/        # source data dirs, shared across executions
│   ├── mariadb118_<compose+dump-hash>/
│   └── target_sqlserver_2026-05-20T10-15-00/   # one target data dir per execution
└── executions/
    ├── matrices/                # matrix run records
    │   └── 2026-05-20T10-00-00_db-comparison/
    │       ├── matrix.yml       # snapshot of the matrix definition that was run
    │       └── matrix-run.json  # start time, status, list of execution timestamps
    ├── 2026-05-20T10-00-00_mysql56-original/
    ├── 2026-05-20T11-15-00_mysql84-original/
    └── 2026-05-20T12-30-00_mariadb118-mysql-upgrade/
```

Each execution directory (whether from a single run or a matrix combination) has the same structure:

```
executions/2026-05-20T10-00-00_mysql56-original/
├── execution.json           # metadata, git refs, jar hash, data dir refs, matrix_run_id if applicable
├── source-docker-compose.yml
├── target-docker-compose.yml
├── jobs/                    # copied from Maven build output of --etl-src
├── datasources/             # copied from Maven build output of --etl-src
├── application.yml          # generated PETL config for this execution
├── job-changes.patch        # if etl-src had uncommitted changes
├── untracked/               # copies of untracked files from etl-src
├── petl.log                 # full PETL stdout/stderr
└── run.json                 # job tree, timings, row counts
```

### Source data directory naming

Source data directories are named `<source-name>_<hash>` where the hash is computed from the content of the source docker-compose file and the dump file. If a data directory with this name already exists, the source database is already provisioned correctly and only needs a container restart (cold cache). If it does not exist, the tool provisions it from scratch.

### Target data directory

Each execution gets a new, empty target data directory named `target_<target-name>_<execution-timestamp>`. Previous execution target data directories are never automatically removed.

---

## CLI Commands

### Run a single execution

```
petl-runner run \
  --source mysql56 \
  --target sqlserver \
  --etl-src ~/code/pih/sl-etl \
  --dump dumps/kgh-2026-05-17.sql \
  [--label mysql56-original] \
  [--petl-jar petl/petl-3.8.0-SNAPSHOT.jar] \
  [--petl-src ~/code/pih/petl]
```

### Run a matrix

```
petl-runner matrix matrices/db-comparison.yml
```

Runs all combinations sequentially. Each combination produces its own timestamped execution directory. A matrix run record is created under `executions/matrices/` grouping them.

### Summarize an execution or matrix run

```
petl-runner summarize [execution-timestamp|matrix-run-id]   # defaults to most recent
```

When given a matrix run id, shows all combinations side by side.

### Compare executions

```
petl-runner compare [run1|matrix-run-id] [run2|matrix-run-id] ...   # defaults to last 2
```

Accepts individual execution timestamps, matrix run ids (expands to all combinations), or a mix of both.

### On-demand checksum

```
petl-runner checksum <execution-timestamp> <table-name>
```

Spins up the target container for the specified execution, computes a checksum for the given table, then stops the container. Run against two executions and compare the output to verify data equivalence.

### Data management

```
petl-runner data list
petl-runner data clean <name>
petl-runner data clean --unreferenced
```

### Execution management

```
petl-runner execution list
petl-runner execution clean <timestamp>
```

---

## Matrix File Format

```yaml
name: db-comparison
combinations:
  - label: mysql56-original
    source: mysql56
    target: sqlserver
    etl-src: ~/code/pih/sl-etl
    dump: dumps/kgh-2026-05-17.sql
  - label: mariadb118-mysql-upgrade
    source: mariadb118
    target: sqlserver
    etl-src: ~/code/pih/sl-etl
    dump: dumps/kgh-2026-05-17.sql
```

Any field that is the same across all combinations can be set as a default in `petl-runner.yml` and omitted from individual entries. Each combination becomes its own execution directory; the matrix definition is snapshotted once into the matrix run record under `executions/matrices/`, not copied into each individual execution.

---

## Run Lifecycle

1. Create timestamped execution directory
2. Snapshot source and target docker-compose files into execution directory
3. Record ETL source git state:
   - If git repo and clean: record commit hash
   - If git repo and dirty: record commit hash + store `git diff HEAD` as `job-changes.patch` + copy untracked files to `untracked/`
   - If not a git repo: copy full directory
4. If `--petl-src` provided: record PETL project git state the same way
5. Record petl jar filename and hash in `execution.json`
6. Compute hash of source docker-compose + dump file
7. If matching source data dir exists: stop the source container if running, then restart it to ensure cold cache
   Else: create data dir, start source container, load dump from file
8. Create new empty target data dir for this execution; start target container
9. Run `mvn clean package` in `--etl-src` directory
10. Copy `jobs/` and `datasources/` from the Maven build output into the execution directory
11. Generate `application.yml` into execution directory wiring:
    - Source datasource (host/port/credentials from source docker-compose)
    - Target datasource (host/port/credentials from target docker-compose)
    - `petl.jobDir` pointing at `<execution-dir>/jobs`
    - `petl.datasourceDir` pointing at `<execution-dir>/datasources`
    - `petl.homeDir` pointing at execution directory
12. Invoke `java -jar <petl-jar>` with generated config, streaming output to `petl.log`
13. On completion (success or failure): query target DB for row counts per table, parse `petl.log` into job tree, write `run.json`
14. Stop target container
15. In matrix mode: if next combination uses the same source config, leave source container running (avoids restart overhead); otherwise stop it

---

## petl.jar Resolution

Resolution order:
1. `--petl-jar` CLI argument (any path)
2. `petl-jar` setting in `petl-runner.yml`
3. If exactly one jar exists in the working directory's `petl/` folder, use it automatically

The jar filename and a content hash are always recorded in `execution.json`. If `--petl-src` is provided, the PETL project git state is also recorded.

---

## ETL Source Tracking

The execution directory captures everything needed to understand exactly what code produced the result:

| State | What is recorded |
|-------|-----------------|
| Clean git repo | Commit hash |
| Dirty git repo | Commit hash + `job-changes.patch` + copied untracked files |
| Not a git repo | Full directory copy |

The copied `jobs/` and `datasources/` directories are always present regardless — these are the exact artifacts that ran.

---

## Log Parsing

`log_parser.py` reads PETL's structured log output and produces a job tree:

- Parses `IN_PROGRESS`, `SUCCEEDED`, `FAILED`, and `ABORTED` events by UUID
- Builds parent-child relationships using timestamp interval containment: if job A's start→end interval contains job B's start→end interval, B is a child of A
- Container jobs (iterating-job, job-pipeline) are rendered as groups with aggregate timing
- If a parent fails due to a child failure, only the child's error details are surfaced — no redundant stack traces from the parent

### summarize output

```
Execution: 2026-05-20T10-15-00  |  mariadb118-mysql-upgrade  |  SUCCEEDED  |  42m 12s

  refresh-ci-warehouse          SUCCEEDED   42m 10s
    ncd_encounter               SUCCEEDED   15m 41s
    mch_labor_progress          SUCCEEDED    4m 38s
    mch_delivery_summary        SUCCEEDED    3m 49s
    all_encounters              SUCCEEDED    3m 05s
    ...
```

---

## Result Storage

### run.json

```json
{
  "label": "mariadb118-mysql-upgrade",
  "timestamp": "2026-05-20T10:15:00",
  "duration_seconds": 2532,
  "status": "SUCCEEDED",
  "config": {
    "source": "mariadb118",
    "source_data_dir": "mariadb118_<hash>",
    "target": "sqlserver",
    "target_data_dir": "target_sqlserver_2026-05-20T10-15-00",
    "dump": "dumps/kgh-2026-05-17.sql",
    "etl_src_git_hash": "abc123",
    "etl_src_dirty": false,
    "petl_jar": "petl-3.8.0-SNAPSHOT.jar",
    "petl_jar_hash": "def456",
    "petl_src_git_hash": null
  },
  "jobs": [],
  "row_counts": {
    "ncd_encounter": 45231,
    "all_patients": 12345
  }
}
```

### execution.json

Written at execution start (before containers or builds) and updated on completion. Contains top-level metadata: label, timestamp, status, source/target config names, data dir references, petl jar filename and hash, etl-src and petl-src git state. Serves as the index record for `execution list` and data management cross-referencing. Does not contain job-level detail.

`run.json` is written only on completion and contains the job tree, per-job timings, and row counts. It is the detailed results record. Both files live in the execution directory.

---

## Compare Output

```
petl-runner compare mysql56-original mariadb118-mysql-upgrade

Job                              | mysql56-orig  | mariadb118-up | delta
---------------------------------|---------------|---------------|------
ncd_encounter                    |    21m 06s    |    15m 41s    |  -26%
mch_labor_progress_encounter     |     7m 18s    |     4m 38s    |  -37%
mch_delivery_summary_encounter   |     7m 02s    |     3m 49s    |  -46%
...
TOTAL                            |  1h 02m 38s   |    42m 12s    |  -33%

Row counts
Table                            | mysql56-orig  | mariadb118-up | match
---------------------------------|---------------|---------------|------
ncd_encounter                    |        45,231 |        45,231 |   YES
all_patients                     |        12,345 |        12,345 |   YES
```

The delta column is relative to the leftmost run. When comparing more than two runs, a delta column is shown for each run relative to the first.

---

## Data Management

### `petl-runner data list`

Shows all source and target data dirs with:
- Name
- Size on disk
- Which executions reference it
- Whether a container is currently running against it

### `petl-runner data clean <name>`

Deletes the specified data dir. If referenced by one or more executions, displays which executions and asks for confirmation before proceeding.

### `petl-runner data clean --unreferenced`

Deletes all data dirs not referenced by any execution. Displays the full list with sizes and asks for confirmation before proceeding.

---

## Execution Management

### `petl-runner execution list`

Shows all executions with label, timestamp, status, and duration.

### `petl-runner execution clean <timestamp>`

Removes the execution directory. If the execution's target data dir is not referenced by any other execution, offers to remove it at the same time rather than leaving it orphaned.

### `petl-runner execution clean --matrix <matrix-run-id>`

Removes the matrix run record and offers to remove all of its execution directories (and any exclusively-referenced target data dirs) in one operation.

---

## Error Handling

- **Container startup failure**: log clearly, fail fast, clean up partial state
- **Dump load failure**: report error, remove partially-created data dir
- **Maven build failure**: report and stop; do not proceed to run
- **PETL failure**: captured in `petl.log` and reflected in `run.json` status; still capture row counts and partial job tree from whatever ran
- **Matrix mode**: on any combination failure, record the failure in that execution's `run.json` and in the matrix run record, then continue with remaining combinations; the matrix run record status reflects the worst outcome across all combinations
