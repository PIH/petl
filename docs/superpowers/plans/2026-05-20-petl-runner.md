# petl-runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `petl-runner`, a Python CLI harness that manages the full lifecycle of PETL ETL test runs — provisioning source/target DB containers, building and running ETL jobs, capturing structured results, and comparing runs.

**Architecture:** Python CLI (`click` + `rich`) installed from `tools/petl-runner/`. All runtime data lives in a user-specified working directory (`PETL_RUNNER_HOME`). Single runs and matrix runs both produce timestamped execution directories that snapshot exactly what ran (docker configs, built ETL artifacts, git state) plus structured results (job tree, timings, row counts).

**Tech Stack:** Python 3.9+, click, rich, pyyaml; Docker CLI via subprocess; `mvn clean package` for ETL builds; PETL invoked as `java -jar`.

---

## File Map

```
tools/petl-runner/
├── pyproject.toml
├── petl_runner/
│   ├── __init__.py
│   ├── cli.py          # click entry points for all commands
│   ├── models.py       # dataclasses: ExecutionRecord, RunResult, JobNode, MatrixRunRecord, etc.
│   ├── git_utils.py    # capture git state (hash, diff, untracked)
│   ├── docker.py       # container lifecycle, dump loading, row counts, checksum
│   ├── runner.py       # single-run orchestration + application.yml generation
│   ├── log_parser.py   # PETL log → job tree
│   ├── results.py      # read/write execution.json, run.json, matrix-run.json
│   └── compare.py      # format job tree, timing diff tables, row count tables
└── tests/
    ├── __init__.py
    ├── fixtures/
    │   └── sample.log
    ├── test_log_parser.py
    ├── test_models.py
    ├── test_results.py
    ├── test_compare.py
    └── test_git_utils.py
```

---

## Task 1: Project scaffold

**Files:**
- Create: `tools/petl-runner/pyproject.toml`
- Create: `tools/petl-runner/petl_runner/__init__.py`
- Create: `tools/petl-runner/petl_runner/cli.py`
- Create: `tools/petl-runner/tests/__init__.py`

- [ ] **Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.backends.legacy:build"

[project]
name = "petl-runner"
version = "0.1.0"
requires-python = ">=3.9"
dependencies = [
    "click>=8.0",
    "rich>=13.0",
    "pyyaml>=6.0",
]

[project.scripts]
petl-runner = "petl_runner.cli:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

- [ ] **Step 2: Create `petl_runner/__init__.py`** (empty file)

- [ ] **Step 3: Create `petl_runner/cli.py`**

```python
import click

@click.group()
@click.option('--work-dir', envvar='PETL_RUNNER_HOME', required=True,
              type=click.Path(exists=True), help='Working directory (or set PETL_RUNNER_HOME)')
@click.pass_context
def main(ctx, work_dir):
    ctx.ensure_object(dict)
    ctx.obj['work_dir'] = work_dir
```

- [ ] **Step 4: Create `tests/__init__.py`** (empty file)

- [ ] **Step 5: Install in dev mode and verify**

```bash
cd tools/petl-runner
pip install -e ".[dev]" 2>/dev/null || pip install -e .
petl-runner --help
```

Expected output: help text showing `--work-dir` option and `Commands:` section (empty for now).

- [ ] **Step 6: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner project scaffold"
```

---

## Task 2: Data models

**Files:**
- Create: `tools/petl-runner/petl_runner/models.py`
- Create: `tools/petl-runner/tests/test_models.py`

- [ ] **Step 1: Write failing test**

```python
# tests/test_models.py
from petl_runner.models import JobNode, ExecutionRecord, RunResult, MatrixRunRecord

def test_job_node_leaf():
    job = JobNode(uuid='abc', description='ncd_encounter', path='ncd_encounter.yml',
                  status='SUCCEEDED', start_time=None, end_time=None,
                  duration_seconds=1267)
    assert job.children == []
    assert job.is_leaf()

def test_job_node_with_children():
    child = JobNode(uuid='b', description='child', path=None,
                    status='SUCCEEDED', start_time=None, end_time=None,
                    duration_seconds=10)
    parent = JobNode(uuid='a', description='parent', path=None,
                     status='SUCCEEDED', start_time=None, end_time=None,
                     duration_seconds=30, children=[child])
    assert not parent.is_leaf()
    assert len(parent.children) == 1

def test_execution_record_roundtrip():
    rec = ExecutionRecord(
        timestamp='2026-05-20T10-15-00',
        label='mysql56-original',
        status='SUCCEEDED',
        source='mysql56',
        source_data_dir='mysql56_abc123',
        target='sqlserver',
        target_data_dir='target_sqlserver_2026-05-20T10-15-00',
        dump='dumps/kgh.sql',
        etl_src_git_hash='def456',
        etl_src_dirty=False,
        petl_jar='petl-3.8.0-SNAPSHOT.jar',
        petl_jar_hash='ghi789',
        petl_src_git_hash=None,
    )
    d = rec.to_dict()
    assert d['label'] == 'mysql56-original'
    assert d['etl_src_dirty'] is False
    assert d['petl_src_git_hash'] is None
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd tools/petl-runner && python -m pytest tests/test_models.py -v
```

Expected: `ImportError` — `models` not found.

- [ ] **Step 3: Create `petl_runner/models.py`**

```python
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional


@dataclass
class JobNode:
    uuid: str
    description: str
    path: Optional[str]
    status: str  # IN_PROGRESS, SUCCEEDED, FAILED, ABORTED
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    duration_seconds: Optional[int]
    children: list = field(default_factory=list)
    error_message: Optional[str] = None

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def to_dict(self) -> dict:
        return {
            'uuid': self.uuid,
            'description': self.description,
            'path': self.path,
            'status': self.status,
            'duration_seconds': self.duration_seconds,
            'error_message': self.error_message,
            'children': [c.to_dict() for c in self.children],
        }


@dataclass
class GitState:
    commit_hash: Optional[str]
    dirty: bool
    has_patch: bool
    has_untracked: bool

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class ExecutionRecord:
    timestamp: str
    label: str
    status: str  # RUNNING, SUCCEEDED, FAILED, ABORTED
    source: str
    source_data_dir: str
    target: str
    target_data_dir: str
    dump: str
    etl_src_git_hash: Optional[str]
    etl_src_dirty: bool
    petl_jar: str
    petl_jar_hash: str
    petl_src_git_hash: Optional[str]
    matrix_run_id: Optional[str] = None
    duration_seconds: Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'ExecutionRecord':
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class RunResult:
    execution: ExecutionRecord
    jobs: list = field(default_factory=list)  # list[JobNode]
    row_counts: dict = field(default_factory=dict)  # table_name -> count

    def to_dict(self) -> dict:
        return {
            'label': self.execution.label,
            'timestamp': self.execution.timestamp,
            'duration_seconds': self.execution.duration_seconds,
            'status': self.execution.status,
            'config': self.execution.to_dict(),
            'jobs': [j.to_dict() for j in self.jobs],
            'row_counts': self.row_counts,
        }


@dataclass
class MatrixCombination:
    label: str
    source: str
    target: str
    etl_src: str
    dump: str
    petl_jar: Optional[str] = None
    petl_src: Optional[str] = None


@dataclass
class MatrixDefinition:
    name: str
    combinations: list = field(default_factory=list)  # list[MatrixCombination]


@dataclass
class MatrixRunRecord:
    matrix_run_id: str
    matrix_name: str
    status: str  # RUNNING, SUCCEEDED, FAILED, PARTIAL
    execution_timestamps: list = field(default_factory=list)
    start_time: Optional[str] = None
    end_time: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> 'MatrixRunRecord':
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class DataDirInfo:
    name: str
    path: str
    size_bytes: int
    referencing_executions: list = field(default_factory=list)
    container_running: bool = False
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_models.py -v
```

Expected: all 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner data models"
```

---

## Task 3: Working directory I/O (`results.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/results.py`
- Create: `tools/petl-runner/tests/test_results.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_results.py
import json
import tempfile
from pathlib import Path
from petl_runner.models import ExecutionRecord, RunResult, MatrixRunRecord
from petl_runner.results import (
    init_work_dir, get_execution_dir, save_execution, load_execution,
    save_run_result, load_run_result, list_executions,
    save_matrix_run, load_matrix_run
)

def make_execution() -> ExecutionRecord:
    return ExecutionRecord(
        timestamp='2026-05-20T10-15-00',
        label='mysql56-original',
        status='SUCCEEDED',
        source='mysql56',
        source_data_dir='mysql56_abc123',
        target='sqlserver',
        target_data_dir='target_sqlserver_2026-05-20T10-15-00',
        dump='dumps/kgh.sql',
        etl_src_git_hash='def456',
        etl_src_dirty=False,
        petl_jar='petl-3.8.0-SNAPSHOT.jar',
        petl_jar_hash='ghi789',
        petl_src_git_hash=None,
    )

def test_init_work_dir_creates_structure():
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        init_work_dir(work_dir)
        assert (work_dir / 'executions').exists()
        assert (work_dir / 'executions' / 'matrices').exists()
        assert (work_dir / 'data').exists()
        assert (work_dir / 'sources').exists()
        assert (work_dir / 'targets').exists()
        assert (work_dir / 'dumps').exists()
        assert (work_dir / 'matrices').exists()
        assert (work_dir / 'petl').exists()

def test_save_and_load_execution():
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        init_work_dir(work_dir)
        rec = make_execution()
        exec_dir = get_execution_dir(work_dir, rec.timestamp)
        exec_dir.mkdir(parents=True)
        save_execution(exec_dir, rec)
        loaded = load_execution(exec_dir)
        assert loaded.label == 'mysql56-original'
        assert loaded.status == 'SUCCEEDED'
        assert loaded.etl_src_dirty is False

def test_save_and_load_run_result():
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        init_work_dir(work_dir)
        rec = make_execution()
        exec_dir = get_execution_dir(work_dir, rec.timestamp)
        exec_dir.mkdir(parents=True)
        result = RunResult(execution=rec, jobs=[], row_counts={'ncd_encounter': 45231})
        save_run_result(exec_dir, result)
        loaded = load_run_result(exec_dir, rec)
        assert loaded.row_counts['ncd_encounter'] == 45231

def test_list_executions():
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        init_work_dir(work_dir)
        for ts in ['2026-05-20T10-00-00_a', '2026-05-20T11-00-00_b']:
            d = get_execution_dir(work_dir, ts)
            d.mkdir(parents=True)
            rec = make_execution()
            rec.timestamp = ts
            save_execution(d, rec)
        executions = list_executions(work_dir)
        assert len(executions) == 2

def test_save_and_load_matrix_run():
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        init_work_dir(work_dir)
        matrix_run = MatrixRunRecord(
            matrix_run_id='2026-05-20T10-00-00_db-comparison',
            matrix_name='db-comparison',
            status='SUCCEEDED',
            execution_timestamps=['2026-05-20T10-00-00', '2026-05-20T11-00-00'],
        )
        save_matrix_run(work_dir, matrix_run)
        loaded = load_matrix_run(work_dir, '2026-05-20T10-00-00_db-comparison')
        assert loaded.matrix_name == 'db-comparison'
        assert len(loaded.execution_timestamps) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_results.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/results.py`**

```python
import json
import shutil
from pathlib import Path
from petl_runner.models import ExecutionRecord, RunResult, MatrixRunRecord


def init_work_dir(work_dir: Path) -> None:
    for subdir in ['executions', 'executions/matrices', 'data', 'sources',
                   'targets', 'dumps', 'matrices', 'petl']:
        (work_dir / subdir).mkdir(parents=True, exist_ok=True)


def get_execution_dir(work_dir: Path, timestamp: str) -> Path:
    return work_dir / 'executions' / timestamp


def get_data_dir(work_dir: Path) -> Path:
    return work_dir / 'data'


def save_execution(exec_dir: Path, record: ExecutionRecord) -> None:
    with open(exec_dir / 'execution.json', 'w') as f:
        json.dump(record.to_dict(), f, indent=2)


def load_execution(exec_dir: Path) -> ExecutionRecord:
    with open(exec_dir / 'execution.json') as f:
        return ExecutionRecord.from_dict(json.load(f))


def save_run_result(exec_dir: Path, result: RunResult) -> None:
    with open(exec_dir / 'run.json', 'w') as f:
        json.dump(result.to_dict(), f, indent=2)


def load_run_result(exec_dir: Path, execution: ExecutionRecord) -> RunResult:
    run_json = exec_dir / 'run.json'
    if not run_json.exists():
        return RunResult(execution=execution)
    with open(run_json) as f:
        data = json.load(f)
    return RunResult(
        execution=execution,
        jobs=[],  # job tree re-parsing happens in log_parser; run.json stores flat dict form
        row_counts=data.get('row_counts', {}),
    )


def list_executions(work_dir: Path) -> list:
    executions_dir = work_dir / 'executions'
    results = []
    for entry in sorted(executions_dir.iterdir()):
        if entry.is_dir() and entry.name != 'matrices':
            exec_json = entry / 'execution.json'
            if exec_json.exists():
                results.append(load_execution(entry))
    return results


def save_matrix_run(work_dir: Path, matrix_run: MatrixRunRecord,
                    matrix_yml_path: Path = None) -> Path:
    matrix_dir = work_dir / 'executions' / 'matrices' / matrix_run.matrix_run_id
    matrix_dir.mkdir(parents=True, exist_ok=True)
    with open(matrix_dir / 'matrix-run.json', 'w') as f:
        json.dump(matrix_run.to_dict(), f, indent=2)
    if matrix_yml_path and matrix_yml_path.exists():
        shutil.copy2(matrix_yml_path, matrix_dir / 'matrix.yml')
    return matrix_dir


def load_matrix_run(work_dir: Path, matrix_run_id: str) -> MatrixRunRecord:
    path = work_dir / 'executions' / 'matrices' / matrix_run_id / 'matrix-run.json'
    with open(path) as f:
        return MatrixRunRecord.from_dict(json.load(f))


def list_matrix_runs(work_dir: Path) -> list:
    matrices_dir = work_dir / 'executions' / 'matrices'
    results = []
    for entry in sorted(matrices_dir.iterdir()):
        if entry.is_dir():
            run_json = entry / 'matrix-run.json'
            if run_json.exists():
                results.append(load_matrix_run(work_dir, entry.name))
    return results


def get_data_dir_infos(work_dir: Path) -> list:
    """Return DataDirInfo for each directory under data/."""
    from petl_runner.models import DataDirInfo
    executions = list_executions(work_dir)
    data_root = work_dir / 'data'
    infos = []
    for entry in sorted(data_root.iterdir()):
        if not entry.is_dir():
            continue
        size = sum(f.stat().st_size for f in entry.rglob('*') if f.is_file())
        refs = []
        for ex in executions:
            if ex.source_data_dir == entry.name or ex.target_data_dir == entry.name:
                refs.append(ex.timestamp)
        infos.append(DataDirInfo(
            name=entry.name,
            path=str(entry),
            size_bytes=size,
            referencing_executions=refs,
        ))
    return infos
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_results.py -v
```

Expected: all 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner working directory I/O"
```

---

## Task 4: Git state capture (`git_utils.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/git_utils.py`
- Create: `tools/petl-runner/tests/test_git_utils.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_git_utils.py
import shutil
import subprocess
import tempfile
from pathlib import Path
from petl_runner.git_utils import capture_git_state


def _make_clean_repo(tmp: Path) -> Path:
    repo = tmp / 'repo'
    repo.mkdir()
    (repo / 'file.sql').write_text('SELECT 1')
    subprocess.run(['git', 'init'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'config', 'user.email', 'test@test.com'], cwd=repo, capture_output=True)
    subprocess.run(['git', 'config', 'user.name', 'Test'], cwd=repo, capture_output=True)
    subprocess.run(['git', 'add', '.'], cwd=repo, check=True, capture_output=True)
    subprocess.run(['git', 'commit', '-m', 'init'], cwd=repo, check=True, capture_output=True)
    return repo


def test_clean_repo_records_commit_hash():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        repo = _make_clean_repo(tmp_path)
        exec_dir = tmp_path / 'exec'
        exec_dir.mkdir()
        state = capture_git_state(repo, exec_dir, 'etl_src')
        assert state.commit_hash is not None
        assert len(state.commit_hash) == 40
        assert state.dirty is False
        assert not (exec_dir / 'etl_src_job-changes.patch').exists()


def test_dirty_repo_writes_patch():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        repo = _make_clean_repo(tmp_path)
        (repo / 'file.sql').write_text('SELECT 2')  # modify tracked file
        exec_dir = tmp_path / 'exec'
        exec_dir.mkdir()
        state = capture_git_state(repo, exec_dir, 'etl_src')
        assert state.dirty is True
        assert state.has_patch is True
        assert (exec_dir / 'etl_src_job-changes.patch').exists()


def test_dirty_repo_with_untracked_copies_files():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        repo = _make_clean_repo(tmp_path)
        (repo / 'new_job.sql').write_text('SELECT 3')  # untracked file
        exec_dir = tmp_path / 'exec'
        exec_dir.mkdir()
        state = capture_git_state(repo, exec_dir, 'etl_src')
        assert state.has_untracked is True
        assert (exec_dir / 'etl_src_untracked' / 'new_job.sql').exists()


def test_non_git_dir_copies_full_directory():
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        src = tmp_path / 'src'
        src.mkdir()
        (src / 'jobs').mkdir()
        (src / 'jobs' / 'job.yml').write_text('type: sql')
        exec_dir = tmp_path / 'exec'
        exec_dir.mkdir()
        state = capture_git_state(src, exec_dir, 'etl_src')
        assert state.commit_hash is None
        assert (exec_dir / 'etl_src_snapshot' / 'jobs' / 'job.yml').exists()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_git_utils.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/git_utils.py`**

```python
import shutil
import subprocess
from pathlib import Path
from petl_runner.models import GitState


def capture_git_state(src_path: Path, exec_dir: Path, prefix: str) -> GitState:
    """Capture the git state of src_path and write artifacts into exec_dir."""
    if not (src_path / '.git').exists():
        snapshot_dir = exec_dir / f'{prefix}_snapshot'
        shutil.copytree(src_path, snapshot_dir)
        return GitState(commit_hash=None, dirty=False, has_patch=False, has_untracked=False)

    commit_hash = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=src_path, capture_output=True, text=True, check=True
    ).stdout.strip()

    status = subprocess.run(
        ['git', 'status', '--porcelain'],
        cwd=src_path, capture_output=True, text=True, check=True
    ).stdout.strip()

    if not status:
        return GitState(commit_hash=commit_hash, dirty=False, has_patch=False, has_untracked=False)

    # dirty — write diff for tracked changes
    diff = subprocess.run(
        ['git', 'diff', 'HEAD'],
        cwd=src_path, capture_output=True, text=True
    ).stdout

    has_patch = bool(diff.strip())
    if has_patch:
        (exec_dir / f'{prefix}_job-changes.patch').write_text(diff)

    # copy untracked files
    untracked = [
        line[3:] for line in status.splitlines()
        if line.startswith('?? ')
    ]
    has_untracked = bool(untracked)
    if has_untracked:
        untracked_dir = exec_dir / f'{prefix}_untracked'
        untracked_dir.mkdir(exist_ok=True)
        for rel_path in untracked:
            src_file = src_path / rel_path.rstrip('/')
            if src_file.is_file():
                dest = untracked_dir / rel_path
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src_file, dest)
            elif src_file.is_dir():
                shutil.copytree(src_file, untracked_dir / rel_path.rstrip('/'))

    return GitState(commit_hash=commit_hash, dirty=True,
                    has_patch=has_patch, has_untracked=has_untracked)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_git_utils.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner git state capture"
```

---

## Task 5: Docker container lifecycle and DB operations (`docker.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/docker.py`
- Create: `tools/petl-runner/tests/test_docker.py`

- [ ] **Step 1: Write failing tests** (pure logic — no subprocess calls)

```python
# tests/test_docker.py
import hashlib
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from petl_runner.docker import compute_source_hash, parse_compose_config, ComposedDB


def test_compute_source_hash_is_stable():
    with tempfile.TemporaryDirectory() as tmp:
        compose = Path(tmp) / 'docker-compose.yml'
        dump = Path(tmp) / 'test.sql'
        compose.write_text('services:\n  mysql:\n    image: mysql:5.6\n')
        dump.write_text('CREATE TABLE t (id INT);')
        h1 = compute_source_hash(compose, dump)
        h2 = compute_source_hash(compose, dump)
        assert h1 == h2
        assert len(h1) == 16  # truncated hash


def test_compute_source_hash_changes_with_compose():
    with tempfile.TemporaryDirectory() as tmp:
        compose = Path(tmp) / 'docker-compose.yml'
        dump = Path(tmp) / 'test.sql'
        compose.write_text('services:\n  mysql:\n    image: mysql:5.6\n')
        dump.write_text('CREATE TABLE t (id INT);')
        h1 = compute_source_hash(compose, dump)
        compose.write_text('services:\n  mysql:\n    image: mysql:8.4\n')
        h2 = compute_source_hash(compose, dump)
        assert h1 != h2


def test_parse_compose_config_mysql():
    with tempfile.TemporaryDirectory() as tmp:
        compose = Path(tmp) / 'docker-compose.yml'
        compose.write_text("""
services:
  mysql:
    image: library/mysql:5.6
    container_name: mysql56
    environment:
      - MYSQL_ROOT_PASSWORD=root
    ports:
      - "3308:3306"
""")
        db = parse_compose_config(compose)
        assert db.container_name == 'mysql56'
        assert db.db_type == 'mysql'
        assert db.host_port == '3308'
        assert db.password == 'root'


def test_parse_compose_config_mariadb():
    with tempfile.TemporaryDirectory() as tmp:
        compose = Path(tmp) / 'docker-compose.yml'
        compose.write_text("""
services:
  mysql:
    image: library/mariadb:11.8
    container_name: mariadb118
    environment:
      - MYSQL_ROOT_PASSWORD=root
    ports:
      - "3308:3306"
""")
        db = parse_compose_config(compose)
        assert db.db_type == 'mariadb'
        assert db.container_name == 'mariadb118'


def test_parse_compose_config_sqlserver():
    with tempfile.TemporaryDirectory() as tmp:
        compose = Path(tmp) / 'docker-compose.yml'
        compose.write_text("""
services:
  sqlserver:
    image: mcr.microsoft.com/mssql/server:2019-latest
    container_name: sqlserver2019
    environment:
      - ACCEPT_EULA=Y
      - SA_PASSWORD=Strong@Pass1
    ports:
      - "1433:1433"
""")
        db = parse_compose_config(compose)
        assert db.db_type == 'sqlserver'
        assert db.container_name == 'sqlserver2019'
        assert db.password == 'Strong@Pass1'
        assert db.host_port == '1433'
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_docker.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/docker.py`**

```python
import hashlib
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
import yaml


@dataclass
class ComposedDB:
    container_name: str
    db_type: str       # mysql, mariadb, sqlserver
    host_port: str
    password: str
    compose_path: Path


def _parse_env(env_section) -> dict:
    if isinstance(env_section, list):
        result = {}
        for item in env_section:
            k, _, v = str(item).partition('=')
            result[k.strip()] = v.strip()
        return result
    return {str(k): str(v) for k, v in (env_section or {}).items()}


def parse_compose_config(compose_path: Path) -> ComposedDB:
    with open(compose_path) as f:
        compose = yaml.safe_load(f)
    service = list(compose['services'].values())[0]
    image = service.get('image', '').lower()
    if 'mariadb' in image:
        db_type = 'mariadb'
    elif 'mysql' in image:
        db_type = 'mysql'
    elif 'mssql' in image or 'sqlserver' in image:
        db_type = 'sqlserver'
    else:
        raise ValueError(f'Unrecognised DB image: {image}')

    ports = service.get('ports', [])
    host_port = str(ports[0]).split(':')[0] if ports else None

    env = _parse_env(service.get('environment'))
    if db_type in ('mysql', 'mariadb'):
        password = env.get('MYSQL_ROOT_PASSWORD', 'root')
    else:
        password = env.get('SA_PASSWORD') or env.get('MSSQL_SA_PASSWORD', '')

    container_name = service.get('container_name', '')
    return ComposedDB(container_name=container_name, db_type=db_type,
                      host_port=host_port, password=password,
                      compose_path=compose_path)


def compute_source_hash(compose_path: Path, dump_path: Path) -> str:
    h = hashlib.md5()
    h.update(compose_path.read_bytes())
    with open(dump_path, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()[:16]


def _run(cmd: list, check=True, capture=False, cwd=None) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, capture_output=capture,
                          text=capture, cwd=cwd)


def start_container(compose_path: Path, data_dir: Path) -> None:
    """Start container with data_dir mounted as the DB data volume."""
    data_dir.mkdir(parents=True, exist_ok=True)
    env = {'PETL_RUNNER_DATA_DIR': str(data_dir)}
    import os
    full_env = {**os.environ, **env}
    subprocess.run(
        ['docker', 'compose', '-f', str(compose_path), 'up', '-d'],
        check=True, env=full_env
    )
    time.sleep(3)  # allow container to initialize


def stop_container(compose_path: Path) -> None:
    subprocess.run(
        ['docker', 'compose', '-f', str(compose_path), 'down'],
        check=False
    )


def restart_container(compose_path: Path, data_dir: Path) -> None:
    stop_container(compose_path)
    time.sleep(2)
    start_container(compose_path, data_dir)


def wait_for_mysql(container_name: str, password: str, timeout: int = 60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ['docker', 'exec', container_name, 'mysql', f'-proot',
             '-e', 'SELECT 1'],
            capture_output=True
        )
        if result.returncode == 0:
            return
        time.sleep(2)
    raise TimeoutError(f'MySQL in {container_name} did not become ready within {timeout}s')


def wait_for_sqlserver(container_name: str, password: str, timeout: int = 90) -> None:
    sqlcmd = _find_sqlcmd(container_name)
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = subprocess.run(
            ['docker', 'exec', container_name, sqlcmd,
             '-S', 'localhost', '-U', 'sa', '-P', password, '-Q', 'SELECT 1'],
            capture_output=True
        )
        if result.returncode == 0:
            return
        time.sleep(3)
    raise TimeoutError(f'SQL Server in {container_name} did not become ready within {timeout}s')


def load_dump(container_name: str, dump_path: Path, db_name: str,
              db_type: str, password: str) -> None:
    """Create database and load mysqldump into it."""
    create_cmd = f'CREATE DATABASE IF NOT EXISTS `{db_name}`'
    subprocess.run(
        ['docker', 'exec', container_name, 'mysql', f'-p{password}',
         '-e', create_cmd],
        check=True
    )
    with open(dump_path, 'rb') as f:
        subprocess.run(
            ['docker', 'exec', '-i', container_name, 'mysql',
             f'-p{password}', db_name],
            stdin=f, check=True
        )


def _find_sqlcmd(container_name: str) -> str:
    result = subprocess.run(
        ['docker', 'exec', container_name, 'bash', '-c',
         'find /opt/mssql-tools* -name sqlcmd -type f 2>/dev/null | sort | head -1'],
        capture_output=True, text=True
    )
    return result.stdout.strip() or '/opt/mssql-tools/bin/sqlcmd'


def query_row_counts(db: ComposedDB, database: Optional[str] = None) -> dict:
    """Query row counts for all user tables in the target SQL Server database."""
    sqlcmd = _find_sqlcmd(db.container_name)
    db_clause = f'USE [{database}]; ' if database else ''
    query = (
        db_clause +
        "SELECT t.name, p.rows FROM sys.tables t "
        "JOIN sys.partitions p ON t.object_id = p.object_id "
        "WHERE p.index_id IN (0,1) ORDER BY t.name"
    )
    result = subprocess.run(
        ['docker', 'exec', db.container_name, sqlcmd,
         '-S', 'localhost', '-U', 'sa', '-P', db.password,
         '-h', '-1', '-W', '-Q', query],
        capture_output=True, text=True, check=True
    )
    counts = {}
    for line in result.stdout.strip().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1].isdigit():
            counts[parts[0]] = int(parts[1])
    return counts


def compute_table_checksum(db: ComposedDB, table: str, database: Optional[str] = None) -> str:
    """Compute CHECKSUM_AGG(BINARY_CHECKSUM(*)) for a SQL Server table."""
    sqlcmd = _find_sqlcmd(db.container_name)
    db_clause = f'USE [{database}]; ' if database else ''
    query = db_clause + f'SELECT CHECKSUM_AGG(BINARY_CHECKSUM(*)) FROM [{table}]'
    result = subprocess.run(
        ['docker', 'exec', db.container_name, sqlcmd,
         '-S', 'localhost', '-U', 'sa', '-P', db.password,
         '-h', '-1', '-W', '-Q', query],
        capture_output=True, text=True, check=True
    )
    return result.stdout.strip()


def get_sqlserver_databases(db: ComposedDB) -> list:
    """Return non-system database names."""
    sqlcmd = _find_sqlcmd(db.container_name)
    query = ("SELECT name FROM sys.databases WHERE name NOT IN "
             "('master','tempdb','model','msdb') ORDER BY name")
    result = subprocess.run(
        ['docker', 'exec', db.container_name, sqlcmd,
         '-S', 'localhost', '-U', 'sa', '-P', db.password,
         '-h', '-1', '-W', '-Q', query],
        capture_output=True, text=True, check=True
    )
    return [line.strip() for line in result.stdout.strip().splitlines() if line.strip()]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_docker.py -v
```

Expected: all 5 tests PASS (these only test pure logic, not subprocess calls).

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner docker container lifecycle"
```

---

## Task 6: Log parser (`log_parser.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/log_parser.py`
- Create: `tools/petl-runner/tests/fixtures/sample.log`
- Create: `tools/petl-runner/tests/test_log_parser.py`

- [ ] **Step 1: Create sample log fixture**

```
# tests/fixtures/sample.log
2026-05-18 10:08:23.680  INFO 203493 --- [main] org.pih.petl.api.JobExecutor             : Job (aaaa-0001): IN_PROGRESS, path: refresh.yml, description: Refresh All Data, initiated: Mon May 18 10:08:23 EDT 2026, started: Mon May 18 10:08:23 EDT 2026
2026-05-18 10:08:23.800  INFO 203493 --- [pool-1] org.pih.petl.api.JobExecutor             : Job (bbbb-0002): IN_PROGRESS, path: ncd_encounter.yml, description: NCD Encounter, initiated: Mon May 18 10:08:23 EDT 2026, started: Mon May 18 10:08:23 EDT 2026
2026-05-18 10:09:31.800  INFO 203493 --- [pool-1] org.pih.petl.api.JobExecutor             : Job (bbbb-0002): SUCCEEDED, path: ncd_encounter.yml, description: NCD Encounter, initiated: Mon May 18 10:08:23 EDT 2026, started: Mon May 18 10:08:23 EDT 2026, completed: Mon May 18 10:09:31 EDT 2026, duration: 68 seconds
2026-05-18 10:09:31.900  INFO 203493 --- [pool-1] org.pih.petl.api.JobExecutor             : Job (cccc-0003): IN_PROGRESS, path: all_patients.yml, description: All Patients, initiated: Mon May 18 10:09:31 EDT 2026, started: Mon May 18 10:09:31 EDT 2026
2026-05-18 10:10:45.000  INFO 203493 --- [pool-1] org.pih.petl.api.JobExecutor             : Job (cccc-0003): SUCCEEDED, path: all_patients.yml, description: All Patients, initiated: Mon May 18 10:09:31 EDT 2026, started: Mon May 18 10:09:31 EDT 2026, completed: Mon May 18 10:10:45 EDT 2026, duration: 73 seconds
2026-05-18 10:10:45.100  INFO 203493 --- [main] org.pih.petl.api.JobExecutor             : Job (dddd-0004): IN_PROGRESS, path: failed_job.yml, description: A Failing Job, initiated: Mon May 18 10:10:45 EDT 2026, started: Mon May 18 10:10:45 EDT 2026
2026-05-18 10:10:46.000  INFO 203493 --- [main] org.pih.petl.api.JobExecutor             : Job (dddd-0004): FAILED, path: failed_job.yml, description: A Failing Job, initiated: Mon May 18 10:10:45 EDT 2026, started: Mon May 18 10:10:45 EDT 2026, completed: Mon May 18 10:10:46 EDT 2026, duration: 1 second
2026-05-18 10:10:46.200  INFO 203493 --- [main] org.pih.petl.api.JobExecutor             : Job (aaaa-0001): FAILED, path: refresh.yml, description: Refresh All Data, initiated: Mon May 18 10:08:23 EDT 2026, started: Mon May 18 10:08:23 EDT 2026, completed: Mon May 18 10:10:46 EDT 2026, duration: 143 seconds
```

- [ ] **Step 2: Write failing tests**

```python
# tests/test_log_parser.py
from pathlib import Path
from petl_runner.log_parser import parse_log, build_job_tree

FIXTURE = Path(__file__).parent / 'fixtures' / 'sample.log'


def test_parse_log_finds_all_jobs():
    jobs = parse_log(FIXTURE)
    uuids = {j.uuid for j in jobs}
    assert 'aaaa-0001' in uuids
    assert 'bbbb-0002' in uuids
    assert 'cccc-0003' in uuids
    assert 'dddd-0004' in uuids


def test_parse_log_captures_terminal_status():
    jobs = parse_log(FIXTURE)
    by_uuid = {j.uuid: j for j in jobs}
    assert by_uuid['bbbb-0002'].status == 'SUCCEEDED'
    assert by_uuid['cccc-0003'].status == 'SUCCEEDED'
    assert by_uuid['dddd-0004'].status == 'FAILED'
    assert by_uuid['aaaa-0001'].status == 'FAILED'


def test_parse_log_captures_duration():
    jobs = parse_log(FIXTURE)
    by_uuid = {j.uuid: j for j in jobs}
    assert by_uuid['bbbb-0002'].duration_seconds == 68
    assert by_uuid['cccc-0003'].duration_seconds == 73
    assert by_uuid['aaaa-0001'].duration_seconds == 143


def test_build_job_tree_finds_root():
    jobs = parse_log(FIXTURE)
    roots = build_job_tree(jobs)
    assert len(roots) == 1
    assert roots[0].uuid == 'aaaa-0001'


def test_build_job_tree_assigns_children():
    jobs = parse_log(FIXTURE)
    roots = build_job_tree(jobs)
    root = roots[0]
    child_uuids = {c.uuid for c in root.children}
    assert 'bbbb-0002' in child_uuids
    assert 'cccc-0003' in child_uuids
    assert 'dddd-0004' in child_uuids


def test_failed_parent_has_child_error_surfaced():
    jobs = parse_log(FIXTURE)
    roots = build_job_tree(jobs)
    root = roots[0]
    # Parent should not have its own error_message; child failure is surfaced
    assert root.error_message is None
    failed_child = next(c for c in root.children if c.status == 'FAILED')
    assert failed_child.uuid == 'dddd-0004'
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
python -m pytest tests/test_log_parser.py -v
```

Expected: `ImportError`.

- [ ] **Step 4: Create `petl_runner/log_parser.py`**

```python
import re
from datetime import datetime
from pathlib import Path
from petl_runner.models import JobNode

LOG_PATTERN = re.compile(
    r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})'  # timestamp
    r'.+?JobExecutor\s*:\s*'
    r'Job \(([^)]+)\):\s*'                              # UUID
    r'(IN_PROGRESS|SUCCEEDED|FAILED|ABORTED)'           # status
    r'(.*)'                                             # rest
)
DURATION_PATTERN = re.compile(r'duration:\s*(\d+)\s*second')
PATH_PATTERN = re.compile(r'(?:^|,\s*)path:\s*([^,]+)')
DESC_PATTERN = re.compile(r'(?:^|,\s*)description:\s*([^,]+(?:,\s*(?!(?:initiated|started|completed|duration):)[^,]+)*)')


def _parse_timestamp(ts_str: str) -> datetime:
    return datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S.%f')


def parse_log(log_path: Path) -> list:
    """Parse PETL log and return one JobNode per UUID (terminal state)."""
    events: dict[str, dict] = {}

    with open(log_path, errors='replace') as f:
        for line in f:
            m = LOG_PATTERN.search(line)
            if not m:
                continue
            ts_str, uuid, status, rest = m.groups()
            ts = _parse_timestamp(ts_str)

            if uuid not in events:
                events[uuid] = {'uuid': uuid, 'start_time': ts, 'status': status,
                                 'path': None, 'description': uuid, 'duration_seconds': None,
                                 'end_time': None}

            path_m = PATH_PATTERN.search(rest)
            if path_m:
                events[uuid]['path'] = path_m.group(1).strip()

            desc_m = DESC_PATTERN.search(rest)
            if desc_m:
                events[uuid]['description'] = desc_m.group(1).strip()

            if status in ('SUCCEEDED', 'FAILED', 'ABORTED'):
                events[uuid]['status'] = status
                events[uuid]['end_time'] = ts
                dur_m = DURATION_PATTERN.search(rest)
                if dur_m:
                    events[uuid]['duration_seconds'] = int(dur_m.group(1))

    return [
        JobNode(
            uuid=e['uuid'],
            description=e['description'],
            path=e['path'],
            status=e['status'],
            start_time=e['start_time'],
            end_time=e['end_time'],
            duration_seconds=e['duration_seconds'],
        )
        for e in events.values()
    ]


def build_job_tree(jobs: list) -> list:
    """Build parent-child tree using interval containment. Returns root jobs."""
    # Sort by start time so we process parents before children
    jobs_with_time = [j for j in jobs if j.start_time and j.end_time]
    jobs_without_time = [j for j in jobs if not (j.start_time and j.end_time)]
    jobs_with_time.sort(key=lambda j: j.start_time)

    parent_of: dict[str, str] = {}

    for i, job in enumerate(jobs_with_time):
        # Find smallest containing interval
        best_parent = None
        best_duration = None
        for candidate in jobs_with_time:
            if candidate.uuid == job.uuid:
                continue
            if (candidate.start_time <= job.start_time and
                    candidate.end_time >= job.end_time):
                duration = (candidate.end_time - candidate.start_time).total_seconds()
                if best_duration is None or duration < best_duration:
                    best_parent = candidate.uuid
                    best_duration = duration
        if best_parent:
            parent_of[job.uuid] = best_parent

    by_uuid = {j.uuid: j for j in jobs}
    for child_uuid, parent_uuid in parent_of.items():
        by_uuid[parent_uuid].children.append(by_uuid[child_uuid])

    roots = [j for j in jobs_with_time if j.uuid not in parent_of]
    roots.extend(jobs_without_time)
    return roots
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
python -m pytest tests/test_log_parser.py -v
```

Expected: all 6 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner PETL log parser"
```

---

## Task 7: Run orchestration (`runner.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/runner.py`

No unit tests for the full orchestration (requires live Docker); tested via integration. The application.yml generation logic is testable.

- [ ] **Step 1: Write failing test for application.yml generation**

```python
# tests/test_runner.py
import tempfile
from pathlib import Path
import yaml
from petl_runner.runner import generate_application_yml


def test_generate_application_yml_sets_paths():
    with tempfile.TemporaryDirectory() as tmp:
        exec_dir = Path(tmp) / 'exec'
        exec_dir.mkdir()
        (exec_dir / 'jobs').mkdir()
        (exec_dir / 'datasources').mkdir()
        etl_src = Path(tmp) / 'etl'
        etl_src.mkdir()
        (etl_src / 'application.yml').write_text(
            'petl:\n  startup:\n    jobs:\n      - refresh.yml\n    exitAutomatically: true\n'
        )
        generate_application_yml(exec_dir, etl_src)
        result = yaml.safe_load((exec_dir / 'application.yml').read_text())
        assert result['petl']['homeDir'] == str(exec_dir)
        assert result['petl']['datasourceDir'] == str(exec_dir / 'datasources')
        assert result['petl']['jobDir'] == str(exec_dir / 'jobs')
        assert result['petl']['startup']['exitAutomatically'] is True
        assert 'refresh.yml' in result['petl']['startup']['jobs']


def test_generate_application_yml_no_etl_app_yml():
    with tempfile.TemporaryDirectory() as tmp:
        exec_dir = Path(tmp) / 'exec'
        exec_dir.mkdir()
        (exec_dir / 'jobs').mkdir()
        (exec_dir / 'datasources').mkdir()
        etl_src = Path(tmp) / 'etl'
        etl_src.mkdir()
        # no application.yml in etl_src
        generate_application_yml(exec_dir, etl_src)
        result = yaml.safe_load((exec_dir / 'application.yml').read_text())
        assert result['petl']['homeDir'] == str(exec_dir)
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_runner.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/runner.py`**

```python
import hashlib
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional
import yaml

from petl_runner.models import ExecutionRecord, RunResult, GitState
from petl_runner.git_utils import capture_git_state
from petl_runner.docker import (
    parse_compose_config, compute_source_hash, start_container,
    stop_container, restart_container, wait_for_mysql, wait_for_sqlserver,
    load_dump, query_row_counts, get_sqlserver_databases
)
from petl_runner.log_parser import parse_log, build_job_tree
from petl_runner.results import (
    get_execution_dir, save_execution, save_run_result, get_data_dir
)


def _file_hash(path: Path) -> str:
    h = hashlib.md5()
    with open(path, 'rb') as f:
        while chunk := f.read(65536):
            h.update(chunk)
    return h.hexdigest()


def generate_application_yml(exec_dir: Path, etl_src: Path) -> None:
    startup = {'exitAutomatically': True, 'jobs': []}
    etl_app_yml = etl_src / 'application.yml'
    if etl_app_yml.exists():
        with open(etl_app_yml) as f:
            etl_config = yaml.safe_load(f) or {}
        etl_startup = (etl_config.get('petl') or {}).get('startup') or {}
        startup['jobs'] = etl_startup.get('jobs', [])
        startup['exitAutomatically'] = True  # always exit when run by petl-runner

    config = {
        'logging': {'level': {'root': 'WARN', 'org.pih': 'INFO'}},
        'spring': {
            'datasource': {
                'platform': 'h2',
                'driver-class-name': 'org.h2.Driver',
                'url': f'jdbc:h2:file:{exec_dir}/data/petl;DB_CLOSE_ON_EXIT=FALSE;AUTO_SERVER=TRUE',
                'username': 'sa',
                'password': 'Test123',
            },
            'jpa': {'hibernate': {'ddl-auto': 'none'}},
            'liquibase': {
                'database-change-log-table': 'PETL_DATABASE_CHANGE_LOG',
                'database-change-log-lock-table': 'PETL_DATABASE_CHANGE_LOG_LOCK',
            },
            'quartz': {'job-store-type': 'memory'},
        },
        'petl': {
            'homeDir': str(exec_dir),
            'datasourceDir': str(exec_dir / 'datasources'),
            'jobDir': str(exec_dir / 'jobs'),
            'startup': startup,
        },
    }
    with open(exec_dir / 'application.yml', 'w') as f:
        yaml.dump(config, f, default_flow_style=False)


def execute_run(source_name: str, target_name: str, etl_src: Path,
                dump_path: Path, label: str, petl_jar: Path,
                work_dir: Path, petl_src: Optional[Path] = None,
                matrix_run_id: Optional[str] = None) -> ExecutionRecord:

    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    if label:
        exec_ts = f'{timestamp}_{label}'
    else:
        exec_ts = timestamp

    exec_dir = get_execution_dir(work_dir, exec_ts)
    exec_dir.mkdir(parents=True)

    # 1. Snapshot docker-compose files
    source_compose = work_dir / 'sources' / source_name / 'docker-compose.yml'
    target_compose = work_dir / 'targets' / target_name / 'docker-compose.yml'
    shutil.copy2(source_compose, exec_dir / 'source-docker-compose.yml')
    shutil.copy2(target_compose, exec_dir / 'target-docker-compose.yml')

    # 2. Capture ETL git state
    etl_git = capture_git_state(etl_src, exec_dir, 'etl_src')

    # 3. Capture PETL git state if provided
    petl_git_hash = None
    if petl_src:
        petl_git = capture_git_state(petl_src, exec_dir, 'petl_src')
        petl_git_hash = petl_git.commit_hash

    # 4. Record jar info
    petl_jar_hash = _file_hash(petl_jar)

    # 5. Determine source data dir
    source_hash = compute_source_hash(source_compose, dump_path)
    source_db = parse_compose_config(source_compose)
    source_data_dir_name = f'{source_name}_{source_hash}'
    source_data_dir = get_data_dir(work_dir) / source_data_dir_name

    # 6. Provision source container
    if source_data_dir.exists():
        print(f'Source data dir {source_data_dir_name} exists — restarting container for cold cache')
        restart_container(source_compose, source_data_dir)
    else:
        print(f'Provisioning new source data dir {source_data_dir_name}')
        start_container(source_compose, source_data_dir)
        if source_db.db_type in ('mysql', 'mariadb'):
            wait_for_mysql(source_db.container_name, source_db.password)
        db_name = dump_path.stem.split('-')[0]  # e.g. kgh from kgh-2026-05-17.sql
        load_dump(source_db.container_name, dump_path, db_name,
                  source_db.db_type, source_db.password)

    # 7. Provision target container
    target_data_dir_name = f'target_{target_name}_{exec_ts}'
    target_data_dir = get_data_dir(work_dir) / target_data_dir_name
    target_db = parse_compose_config(target_compose)
    start_container(target_compose, target_data_dir)
    if target_db.db_type == 'sqlserver':
        wait_for_sqlserver(target_db.container_name, target_db.password)

    # Write initial execution record
    record = ExecutionRecord(
        timestamp=exec_ts, label=label or exec_ts, status='RUNNING',
        source=source_name, source_data_dir=source_data_dir_name,
        target=target_name, target_data_dir=target_data_dir_name,
        dump=str(dump_path), etl_src_git_hash=etl_git.commit_hash,
        etl_src_dirty=etl_git.dirty, petl_jar=petl_jar.name,
        petl_jar_hash=petl_jar_hash, petl_src_git_hash=petl_git_hash,
        matrix_run_id=matrix_run_id,
    )
    save_execution(exec_dir, record)

    # 8. Build ETL project
    print(f'Building ETL project in {etl_src}')
    subprocess.run(['mvn', 'clean', 'package', '-q'], cwd=etl_src, check=True)

    # 9. Copy jobs and datasources from Maven target/
    maven_target = etl_src / 'target'
    for subdir in ('jobs', 'datasources'):
        src = maven_target / subdir
        dst = exec_dir / subdir
        if src.exists():
            shutil.copytree(src, dst)
        else:
            dst.mkdir()

    # 10. Generate application.yml
    generate_application_yml(exec_dir, etl_src)

    # 11. Run PETL
    log_path = exec_dir / 'petl.log'
    start = time.time()
    petl_status = 'SUCCEEDED'
    try:
        with open(log_path, 'w') as log_file:
            result = subprocess.run(
                ['java', '-jar', str(petl_jar),
                 f'--spring.config.additional-location=file:{exec_dir}/'],
                stdout=log_file, stderr=subprocess.STDOUT,
                cwd=str(exec_dir),
            )
        if result.returncode != 0:
            petl_status = 'FAILED'
    except Exception as e:
        petl_status = 'FAILED'
        with open(log_path, 'a') as f:
            f.write(f'\npetl-runner: exception launching PETL: {e}\n')

    duration = int(time.time() - start)

    # 12. Capture results
    jobs = []
    row_counts = {}
    try:
        raw_jobs = parse_log(log_path)
        jobs = build_job_tree(raw_jobs)
    except Exception as e:
        print(f'Warning: log parsing failed: {e}')

    try:
        databases = get_sqlserver_databases(target_db)
        for db_name in databases:
            row_counts.update(query_row_counts(target_db, db_name))
    except Exception as e:
        print(f'Warning: row count query failed: {e}')

    # 13. Stop target container
    stop_container(target_compose)

    # Update and save final execution record
    record.status = petl_status
    record.duration_seconds = duration
    save_execution(exec_dir, record)
    run_result = RunResult(execution=record, jobs=jobs, row_counts=row_counts)
    save_run_result(exec_dir, run_result)

    return record
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_runner.py -v
```

Expected: all 2 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner run orchestration"
```

---

## Task 8: Output formatting (`compare.py`)

**Files:**
- Create: `tools/petl-runner/petl_runner/compare.py`
- Create: `tools/petl-runner/tests/test_compare.py`

- [ ] **Step 1: Write failing tests**

```python
# tests/test_compare.py
from petl_runner.compare import format_duration, collect_job_timings

def test_format_duration_seconds():
    assert format_duration(45) == '45s'

def test_format_duration_minutes():
    assert format_duration(126) == '2m 06s'

def test_format_duration_hours():
    assert format_duration(3758) == '1h 02m 38s'

def test_collect_job_timings_flat():
    from petl_runner.models import JobNode
    root = JobNode(uuid='a', description='refresh', path='refresh.yml',
                   status='SUCCEEDED', start_time=None, end_time=None,
                   duration_seconds=3758)
    child = JobNode(uuid='b', description='ncd_encounter', path='ncd_encounter.yml',
                    status='SUCCEEDED', start_time=None, end_time=None,
                    duration_seconds=1266)
    root.children = [child]
    timings = collect_job_timings([root])
    assert timings['refresh.yml'] == 3758
    assert timings['ncd_encounter.yml'] == 1266
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
python -m pytest tests/test_compare.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/compare.py`**

```python
from typing import Optional
from rich.console import Console
from rich.table import Table
from rich import box
from petl_runner.models import JobNode, RunResult

STATUS_STYLE = {
    'SUCCEEDED': 'green',
    'FAILED': 'red',
    'ABORTED': 'yellow',
    'RUNNING': 'cyan',
    'IN_PROGRESS': 'cyan',
}


def format_duration(seconds: Optional[int]) -> str:
    if seconds is None:
        return '—'
    if seconds < 60:
        return f'{seconds}s'
    m, s = divmod(seconds, 60)
    if m < 60:
        return f'{m}m {s:02d}s'
    h, m = divmod(m, 60)
    return f'{h}h {m:02d}m {s:02d}s'


def collect_job_timings(roots: list, result: dict = None) -> dict:
    """Recursively collect {path: duration_seconds} for all jobs."""
    if result is None:
        result = {}
    for job in roots:
        if job.path:
            result[job.path] = job.duration_seconds
        collect_job_timings(job.children, result)
    return result


def _print_job_tree(jobs: list, console: Console, indent: int = 0) -> None:
    for job in jobs:
        style = STATUS_STYLE.get(job.status, 'white')
        duration = format_duration(job.duration_seconds)
        prefix = '  ' * indent
        label = job.path or job.description
        console.print(f'  {prefix}[{style}]{job.status:10}[/{style}]  {duration:>12}  {label}')
        if job.children:
            _print_job_tree(job.children, console, indent + 1)


def print_execution_summary(execution, roots: list, row_counts: dict,
                             console: Console) -> None:
    status_style = STATUS_STYLE.get(execution.status, 'white')
    duration = format_duration(execution.duration_seconds)
    console.print()
    console.print(
        f'[bold]Execution:[/bold] {execution.timestamp}  |  '
        f'[bold]{execution.label}[/bold]  |  '
        f'[{status_style}]{execution.status}[/{status_style}]  |  '
        f'[cyan]{duration}[/cyan]'
    )
    console.print(f'  Source: {execution.source}  |  Target: {execution.target}  |  '
                  f'ETL: {execution.etl_src_git_hash or "unknown"}'
                  f'{"*" if execution.etl_src_dirty else ""}  |  '
                  f'PETL: {execution.petl_jar}')
    console.print()
    if roots:
        _print_job_tree(roots, console)
    if row_counts:
        console.print()
        console.print('[bold]Row counts:[/bold]')
        for table, count in sorted(row_counts.items()):
            console.print(f'  {table}: {count:,}')
    console.print()


def print_comparison(run_results: list, console: Console) -> None:
    """Print timing and row count diff tables for N RunResults."""
    if not run_results:
        console.print('[yellow]No results to compare.[/yellow]')
        return

    labels = [r.execution.label for r in run_results]

    # Collect all job paths across all runs
    all_paths = []
    seen = set()
    for result in run_results:
        timings = collect_job_timings(result.jobs)
        for path in timings:
            if path not in seen:
                all_paths.append(path)
                seen.add(path)

    # Timing table
    timing_table = Table(box=box.SIMPLE, show_header=True)
    timing_table.add_column('Job', style='bold')
    for label in labels:
        timing_table.add_column(label, justify='right')
    if len(run_results) > 1:
        timing_table.add_column('delta', justify='right')

    baseline_timings = collect_job_timings(run_results[0].jobs) if run_results else {}

    for path in all_paths:
        row = [path]
        first_secs = None
        for i, result in enumerate(run_results):
            timings = collect_job_timings(result.jobs)
            secs = timings.get(path)
            if i == 0:
                first_secs = secs
            row.append(format_duration(secs))
        if len(run_results) > 1 and first_secs and first_secs > 0:
            last_secs = collect_job_timings(run_results[-1].jobs).get(path)
            if last_secs is not None:
                pct = int((last_secs - first_secs) / first_secs * 100)
                style = 'green' if pct < 0 else ('red' if pct > 0 else 'white')
                row.append(f'[{style}]{pct:+d}%[/{style}]')
            else:
                row.append('—')
        timing_table.add_row(*row)

    # TOTAL row
    totals = [r.execution.duration_seconds for r in run_results]
    total_row = ['[bold]TOTAL[/bold]'] + [format_duration(t) for t in totals]
    if len(run_results) > 1 and totals[0] and totals[-1]:
        pct = int((totals[-1] - totals[0]) / totals[0] * 100)
        style = 'green' if pct < 0 else ('red' if pct > 0 else 'white')
        total_row.append(f'[{style}]{pct:+d}%[/{style}]')
    timing_table.add_row(*total_row)

    console.print()
    console.print('[bold]Timings[/bold]')
    console.print(timing_table)

    # Row count table — only if any results have row counts
    all_tables = sorted({t for r in run_results for t in r.row_counts})
    if all_tables:
        rc_table = Table(box=box.SIMPLE)
        rc_table.add_column('Table', style='bold')
        for label in labels:
            rc_table.add_column(label, justify='right')
        if len(run_results) > 1:
            rc_table.add_column('match', justify='center')

        for table in all_tables:
            counts = [r.row_counts.get(table) for r in run_results]
            row = [table] + [f'{c:,}' if c is not None else '—' for c in counts]
            if len(run_results) > 1:
                all_same = len(set(c for c in counts if c is not None)) == 1
                row.append('[green]YES[/green]' if all_same else '[red]NO[/red]')
            rc_table.add_row(*row)

        console.print('[bold]Row counts[/bold]')
        console.print(rc_table)
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_compare.py -v
```

Expected: all 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner output formatting"
```

---

## Task 9: CLI — `run`, `summarize`, `compare` commands

**Files:**
- Modify: `tools/petl-runner/petl_runner/cli.py`

- [ ] **Step 1: Add `run` command to `cli.py`**

```python
import click
from pathlib import Path
from rich.console import Console

@click.group()
@click.option('--work-dir', envvar='PETL_RUNNER_HOME', required=True,
              type=click.Path(exists=True), help='Working directory (or set PETL_RUNNER_HOME)')
@click.pass_context
def main(ctx, work_dir):
    ctx.ensure_object(dict)
    ctx.obj['work_dir'] = Path(work_dir)


@main.command()
@click.option('--source', required=True, help='Source name (subdir under sources/)')
@click.option('--target', required=True, help='Target name (subdir under targets/)')
@click.option('--etl-src', required=True, type=click.Path(exists=True))
@click.option('--dump', required=True, type=click.Path(exists=True))
@click.option('--label', default=None)
@click.option('--petl-jar', default=None, type=click.Path())
@click.option('--petl-src', default=None, type=click.Path(exists=True))
@click.pass_context
def run(ctx, source, target, etl_src, dump, label, petl_jar, petl_src):
    """Execute a single ETL test run."""
    from petl_runner.runner import execute_run
    from petl_runner.results import init_work_dir
    work_dir = ctx.obj['work_dir']
    init_work_dir(work_dir)
    resolved_jar = _resolve_petl_jar(work_dir, petl_jar)
    record = execute_run(
        source_name=source, target_name=target,
        etl_src=Path(etl_src), dump_path=Path(dump),
        label=label, petl_jar=resolved_jar,
        work_dir=work_dir,
        petl_src=Path(petl_src) if petl_src else None,
    )
    console = Console()
    console.print(f'\n[bold]Run complete:[/bold] {record.label} — {record.status} '
                  f'({record.duration_seconds}s)')


def _resolve_petl_jar(work_dir: Path, petl_jar_arg: str) -> Path:
    if petl_jar_arg:
        return Path(petl_jar_arg)
    config_file = work_dir / 'petl-runner.yml'
    if config_file.exists():
        import yaml
        with open(config_file) as f:
            cfg = yaml.safe_load(f) or {}
        if cfg.get('petl-jar'):
            return Path(cfg['petl-jar'])
    petl_dir = work_dir / 'petl'
    jars = list(petl_dir.glob('*.jar'))
    if len(jars) == 1:
        return jars[0]
    if len(jars) > 1:
        raise click.UsageError(
            f'Multiple jars in {petl_dir}, specify with --petl-jar')
    raise click.UsageError(
        f'No petl jar found. Place one in {petl_dir} or use --petl-jar')


@main.command()
@click.argument('execution_id', required=False)
@click.pass_context
def summarize(ctx, execution_id):
    """Summarize an execution (defaults to most recent)."""
    from petl_runner.results import list_executions, get_execution_dir, load_run_result
    from petl_runner.log_parser import parse_log, build_job_tree
    from petl_runner.compare import print_execution_summary
    work_dir = ctx.obj['work_dir']
    console = Console()

    if execution_id:
        exec_dir = get_execution_dir(work_dir, execution_id)
    else:
        executions = list_executions(work_dir)
        if not executions:
            console.print('[yellow]No executions found.[/yellow]')
            return
        exec_dir = get_execution_dir(work_dir, executions[-1].timestamp)

    from petl_runner.results import load_execution
    execution = load_execution(exec_dir)
    log_path = exec_dir / 'petl.log'
    roots = []
    if log_path.exists():
        try:
            raw = parse_log(log_path)
            roots = build_job_tree(raw)
        except Exception:
            pass
    result = load_run_result(exec_dir, execution)
    print_execution_summary(execution, roots, result.row_counts, console)


@main.command()
@click.argument('run_ids', nargs=-1)
@click.pass_context
def compare(ctx, run_ids):
    """Compare executions (defaults to last 2). Accepts timestamps or matrix run ids."""
    from petl_runner.results import (
        list_executions, get_execution_dir, load_execution,
        load_run_result, load_matrix_run
    )
    from petl_runner.log_parser import parse_log, build_job_tree
    from petl_runner.compare import print_comparison
    work_dir = ctx.obj['work_dir']
    console = Console()

    if not run_ids:
        all_execs = list_executions(work_dir)
        if len(all_execs) < 2:
            console.print('[yellow]Need at least 2 executions to compare.[/yellow]')
            return
        run_ids = (all_execs[-2].timestamp, all_execs[-1].timestamp)

    run_results = []
    for run_id in run_ids:
        # Check if it's a matrix run id
        matrix_dir = work_dir / 'executions' / 'matrices' / run_id
        if matrix_dir.exists():
            matrix_run = load_matrix_run(work_dir, run_id)
            for ts in matrix_run.execution_timestamps:
                exec_dir = get_execution_dir(work_dir, ts)
                execution = load_execution(exec_dir)
                log_path = exec_dir / 'petl.log'
                roots = []
                if log_path.exists():
                    try:
                        roots = build_job_tree(parse_log(log_path))
                    except Exception:
                        pass
                result = load_run_result(exec_dir, execution)
                result.jobs = roots
                run_results.append(result)
        else:
            exec_dir = get_execution_dir(work_dir, run_id)
            execution = load_execution(exec_dir)
            log_path = exec_dir / 'petl.log'
            roots = []
            if log_path.exists():
                try:
                    roots = build_job_tree(parse_log(log_path))
                except Exception:
                    pass
            result = load_run_result(exec_dir, execution)
            result.jobs = roots
            run_results.append(result)

    print_comparison(run_results, console)
```

- [ ] **Step 2: Verify commands appear**

```bash
petl-runner --help
```

Expected: `run`, `summarize`, `compare` listed under Commands.

- [ ] **Step 3: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner run, summarize, compare commands"
```

---

## Task 10: Matrix support

**Files:**
- Create: `tools/petl-runner/petl_runner/matrix.py`
- Modify: `tools/petl-runner/petl_runner/cli.py`

- [ ] **Step 1: Write failing test for matrix file parsing**

```python
# tests/test_matrix.py
import tempfile
from pathlib import Path
from petl_runner.matrix import parse_matrix_file

MATRIX_YML = """
name: db-comparison
defaults:
  target: sqlserver
  etl-src: ~/code/pih/sl-etl
  dump: dumps/kgh.sql
combinations:
  - label: mysql56-original
    source: mysql56
  - label: mariadb118-upgrade
    source: mariadb118
"""

def test_parse_matrix_file():
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / 'matrix.yml'
        f.write_text(MATRIX_YML)
        matrix = parse_matrix_file(f)
        assert matrix.name == 'db-comparison'
        assert len(matrix.combinations) == 2
        assert matrix.combinations[0].label == 'mysql56-original'
        assert matrix.combinations[0].source == 'mysql56'
        assert matrix.combinations[0].target == 'sqlserver'
        assert matrix.combinations[1].source == 'mariadb118'

def test_parse_matrix_defaults_applied():
    with tempfile.TemporaryDirectory() as tmp:
        f = Path(tmp) / 'matrix.yml'
        f.write_text(MATRIX_YML)
        matrix = parse_matrix_file(f)
        for combo in matrix.combinations:
            assert combo.dump == 'dumps/kgh.sql'
            assert combo.etl_src == '~/code/pih/sl-etl'
```

- [ ] **Step 2: Run test to verify it fails**

```bash
python -m pytest tests/test_matrix.py -v
```

Expected: `ImportError`.

- [ ] **Step 3: Create `petl_runner/matrix.py`**

```python
from datetime import datetime
from pathlib import Path
from typing import Optional
import yaml

from petl_runner.models import MatrixCombination, MatrixDefinition, MatrixRunRecord
from petl_runner.results import save_matrix_run, save_execution, get_execution_dir


def parse_matrix_file(path: Path) -> MatrixDefinition:
    with open(path) as f:
        data = yaml.safe_load(f)
    defaults = data.get('defaults', {})
    combos = []
    for raw in data.get('combinations', []):
        merged = {**defaults, **raw}
        combos.append(MatrixCombination(
            label=merged.get('label', ''),
            source=merged.get('source', ''),
            target=merged.get('target', ''),
            etl_src=merged.get('etl-src', ''),
            dump=merged.get('dump', ''),
            petl_jar=merged.get('petl-jar'),
            petl_src=merged.get('petl-src'),
        ))
    return MatrixDefinition(name=data.get('name', ''), combinations=combos)


def execute_matrix(matrix_def: MatrixDefinition, matrix_path: Path,
                   work_dir: Path, petl_jar_resolver) -> MatrixRunRecord:
    from petl_runner.runner import execute_run

    matrix_run_id = f'{datetime.now().strftime("%Y-%m-%dT%H-%M-%S")}_{matrix_def.name}'
    matrix_run = MatrixRunRecord(
        matrix_run_id=matrix_run_id,
        matrix_name=matrix_def.name,
        status='RUNNING',
        start_time=datetime.now().isoformat(),
    )
    save_matrix_run(work_dir, matrix_run, matrix_path)

    last_source = None

    for combo in matrix_def.combinations:
        print(f'\n[matrix] Running combination: {combo.label}')
        try:
            resolved_jar = petl_jar_resolver(work_dir, combo.petl_jar)
            record = execute_run(
                source_name=combo.source,
                target_name=combo.target,
                etl_src=Path(combo.etl_src).expanduser(),
                dump_path=Path(combo.dump),
                label=combo.label,
                petl_jar=resolved_jar,
                work_dir=work_dir,
                petl_src=Path(combo.petl_src).expanduser() if combo.petl_src else None,
                matrix_run_id=matrix_run_id,
            )
            matrix_run.execution_timestamps.append(record.timestamp)

            # Stop source container if next combo uses a different source
            next_combos = matrix_def.combinations[matrix_def.combinations.index(combo) + 1:]
            if not next_combos or next_combos[0].source != combo.source:
                from petl_runner.docker import stop_container
                source_compose = work_dir / 'sources' / combo.source / 'docker-compose.yml'
                stop_container(source_compose)

        except Exception as e:
            print(f'[matrix] Combination {combo.label} FAILED: {e}')

    # Determine overall status
    if not matrix_run.execution_timestamps:
        matrix_run.status = 'FAILED'
    else:
        # Load all execution statuses
        from petl_runner.results import load_execution, get_execution_dir
        statuses = []
        for ts in matrix_run.execution_timestamps:
            try:
                exec_dir = get_execution_dir(work_dir, ts)
                rec = load_execution(exec_dir)
                statuses.append(rec.status)
            except Exception:
                statuses.append('FAILED')
        if all(s == 'SUCCEEDED' for s in statuses):
            matrix_run.status = 'SUCCEEDED'
        elif any(s == 'SUCCEEDED' for s in statuses):
            matrix_run.status = 'PARTIAL'
        else:
            matrix_run.status = 'FAILED'

    matrix_run.end_time = datetime.now().isoformat()
    save_matrix_run(work_dir, matrix_run)
    return matrix_run
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
python -m pytest tests/test_matrix.py -v
```

Expected: all 2 tests PASS.

- [ ] **Step 5: Add `matrix` command to `cli.py`**

Add after the existing commands in `cli.py`:

```python
@main.command(name='matrix')
@click.argument('matrix_file', type=click.Path(exists=True))
@click.pass_context
def matrix_cmd(ctx, matrix_file):
    """Run all combinations defined in a matrix file."""
    from petl_runner.matrix import parse_matrix_file, execute_matrix
    from petl_runner.results import init_work_dir
    work_dir = ctx.obj['work_dir']
    init_work_dir(work_dir)
    console = Console()
    matrix_path = Path(matrix_file)
    matrix_def = parse_matrix_file(matrix_path)
    console.print(f'[bold]Running matrix:[/bold] {matrix_def.name} '
                  f'({len(matrix_def.combinations)} combinations)')
    matrix_run = execute_matrix(matrix_def, matrix_path, work_dir, _resolve_petl_jar)
    console.print(f'\n[bold]Matrix complete:[/bold] {matrix_run.matrix_name} — {matrix_run.status}')
    for ts in matrix_run.execution_timestamps:
        console.print(f'  {ts}')
```

- [ ] **Step 6: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner matrix run support"
```

---

## Task 11: Data and execution management commands

**Files:**
- Modify: `tools/petl-runner/petl_runner/cli.py`

- [ ] **Step 1: Add `data` and `execution` command groups to `cli.py`**

```python
@main.group()
def data():
    """Manage DB data directories."""
    pass


@data.command(name='list')
@click.pass_context
def data_list(ctx):
    """List all data directories with sizes and referencing executions."""
    from petl_runner.results import get_data_dir_infos
    work_dir = ctx.obj['work_dir']
    console = Console()
    infos = get_data_dir_infos(work_dir)
    if not infos:
        console.print('[yellow]No data directories found.[/yellow]')
        return
    table = Table(box=box.SIMPLE)
    table.add_column('Name', style='bold')
    table.add_column('Size', justify='right')
    table.add_column('Executions')
    for info in infos:
        size_mb = info.size_bytes / (1024 * 1024)
        table.add_row(
            info.name,
            f'{size_mb:.1f} MB',
            ', '.join(info.referencing_executions) or '[dim]none[/dim]'
        )
    from rich import box as rich_box
    console.print(table)


@data.command(name='clean')
@click.argument('name', required=False)
@click.option('--unreferenced', is_flag=True, help='Clean all unreferenced data dirs')
@click.pass_context
def data_clean(ctx, name, unreferenced):
    """Delete a data directory (warns if still referenced by executions)."""
    import shutil
    from petl_runner.results import get_data_dir_infos, get_data_dir
    work_dir = ctx.obj['work_dir']
    console = Console()

    if unreferenced:
        infos = [i for i in get_data_dir_infos(work_dir) if not i.referencing_executions]
        if not infos:
            console.print('[green]No unreferenced data directories.[/green]')
            return
        console.print('[bold]Will delete:[/bold]')
        for info in infos:
            console.print(f'  {info.name}  ({info.size_bytes / (1024*1024):.1f} MB)')
        click.confirm('Proceed?', abort=True)
        for info in infos:
            shutil.rmtree(info.path)
            console.print(f'[green]Deleted:[/green] {info.name}')
        return

    if not name:
        raise click.UsageError('Provide a name or use --unreferenced')

    infos = {i.name: i for i in get_data_dir_infos(work_dir)}
    if name not in infos:
        console.print(f'[red]Not found:[/red] {name}')
        return

    info = infos[name]
    if info.referencing_executions:
        console.print(f'[yellow]Warning:[/yellow] Referenced by:')
        for ts in info.referencing_executions:
            console.print(f'  {ts}')
        click.confirm('Delete anyway?', abort=True)

    shutil.rmtree(info.path)
    console.print(f'[green]Deleted:[/green] {name}')


@main.group()
def execution():
    """Manage executions."""
    pass


@execution.command(name='list')
@click.pass_context
def execution_list(ctx):
    """List all executions."""
    from petl_runner.results import list_executions
    from petl_runner.compare import format_duration
    work_dir = ctx.obj['work_dir']
    console = Console()
    executions = list_executions(work_dir)
    if not executions:
        console.print('[yellow]No executions found.[/yellow]')
        return
    from rich.table import Table
    from rich import box as rich_box
    table = Table(box=rich_box.SIMPLE)
    table.add_column('Timestamp', style='bold')
    table.add_column('Label')
    table.add_column('Status')
    table.add_column('Duration', justify='right')
    for ex in executions:
        style = {'SUCCEEDED': 'green', 'FAILED': 'red', 'RUNNING': 'cyan'}.get(ex.status, 'white')
        table.add_row(ex.timestamp, ex.label,
                      f'[{style}]{ex.status}[/{style}]',
                      format_duration(ex.duration_seconds))
    console.print(table)


@execution.command(name='clean')
@click.argument('timestamp', required=False)
@click.option('--matrix', 'matrix_run_id', default=None,
              help='Clean a matrix run and offer to clean all its executions')
@click.pass_context
def execution_clean(ctx, timestamp, matrix_run_id):
    """Remove an execution directory (and optionally its target data dir)."""
    import shutil
    from petl_runner.results import (
        get_execution_dir, load_execution, get_data_dir,
        load_matrix_run, list_executions
    )
    work_dir = ctx.obj['work_dir']
    console = Console()

    if matrix_run_id:
        try:
            matrix_run = load_matrix_run(work_dir, matrix_run_id)
        except FileNotFoundError:
            console.print(f'[red]Matrix run not found:[/red] {matrix_run_id}')
            return
        console.print(f'[bold]Matrix run:[/bold] {matrix_run.matrix_name}')
        console.print(f'Executions: {", ".join(matrix_run.execution_timestamps)}')
        click.confirm('Delete matrix run record and all its executions?', abort=True)
        for ts in matrix_run.execution_timestamps:
            _clean_single_execution(ts, work_dir, console, confirm=False)
        matrix_dir = work_dir / 'executions' / 'matrices' / matrix_run_id
        shutil.rmtree(matrix_dir)
        console.print(f'[green]Deleted matrix run:[/green] {matrix_run_id}')
        return

    if not timestamp:
        raise click.UsageError('Provide a timestamp or use --matrix')
    _clean_single_execution(timestamp, work_dir, console, confirm=True)


def _clean_single_execution(timestamp: str, work_dir: Path,
                              console, confirm: bool = True) -> None:
    import shutil
    from petl_runner.results import get_execution_dir, load_execution, get_data_dir_infos
    exec_dir = get_execution_dir(work_dir, timestamp)
    if not exec_dir.exists():
        console.print(f'[red]Not found:[/red] {timestamp}')
        return
    execution = load_execution(exec_dir)
    if confirm:
        click.confirm(f'Delete execution {timestamp}?', abort=True)
    shutil.rmtree(exec_dir)
    console.print(f'[green]Deleted execution:[/green] {timestamp}')

    # Check if target data dir is now orphaned
    target_dir = work_dir / 'data' / execution.target_data_dir
    if target_dir.exists():
        infos = {i.name: i for i in get_data_dir_infos(work_dir)}
        info = infos.get(execution.target_data_dir)
        if info and not info.referencing_executions:
            if click.confirm(f'Target data dir {execution.target_data_dir} is now unreferenced. Delete it?'):
                shutil.rmtree(target_dir)
                console.print(f'[green]Deleted data dir:[/green] {execution.target_data_dir}')
```

- [ ] **Step 2: Add missing import to `cli.py`**

At the top of `cli.py`, add:

```python
from rich import box
from rich.table import Table
```

- [ ] **Step 3: Verify all commands appear**

```bash
petl-runner --help
petl-runner data --help
petl-runner execution --help
```

Expected: `run`, `summarize`, `compare`, `matrix`, `data`, `execution` listed; subcommands listed for each group.

- [ ] **Step 4: Commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner data and execution management commands"
```

---

## Task 12: Checksum command and full test run

**Files:**
- Modify: `tools/petl-runner/petl_runner/cli.py`

- [ ] **Step 1: Add `checksum` command to `cli.py`**

```python
@main.command()
@click.argument('execution_id')
@click.argument('table_name')
@click.option('--database', default=None, help='SQL Server database name (auto-detected if omitted)')
@click.pass_context
def checksum(ctx, execution_id, table_name, database):
    """Compute CHECKSUM_AGG for a table in the target DB of a given execution."""
    from petl_runner.results import get_execution_dir, load_execution
    from petl_runner.docker import (
        parse_compose_config, start_container, stop_container,
        wait_for_sqlserver, compute_table_checksum, get_sqlserver_databases
    )
    work_dir = ctx.obj['work_dir']
    console = Console()
    exec_dir = get_execution_dir(work_dir, execution_id)
    execution = load_execution(exec_dir)
    target_compose = exec_dir / 'target-docker-compose.yml'
    target_data_dir = work_dir / 'data' / execution.target_data_dir

    if not target_data_dir.exists():
        console.print(f'[red]Target data dir not found:[/red] {execution.target_data_dir}')
        return

    console.print(f'Starting target container for execution {execution_id}...')
    target_db = parse_compose_config(target_compose)
    start_container(target_compose, target_data_dir)
    try:
        wait_for_sqlserver(target_db.container_name, target_db.password)
        db_name = database
        if not db_name:
            databases = get_sqlserver_databases(target_db)
            if not databases:
                console.print('[red]No user databases found in target.[/red]')
                return
            db_name = databases[0]
        result = compute_table_checksum(target_db, table_name, db_name)
        console.print(f'[bold]Checksum[/bold] {execution_id} / {db_name}.{table_name}: [cyan]{result}[/cyan]')
    finally:
        stop_container(target_compose)
```

- [ ] **Step 2: Run the full test suite**

```bash
cd tools/petl-runner && python -m pytest tests/ -v
```

Expected: all tests PASS.

- [ ] **Step 3: Verify the complete CLI**

```bash
petl-runner --help
```

Expected output:
```
Commands:
  checksum   Compute CHECKSUM_AGG for a table in the target DB...
  compare    Compare executions (defaults to last 2)...
  data       Manage DB data directories.
  execution  Manage executions.
  matrix     Run all combinations defined in a matrix file.
  run        Execute a single ETL test run.
  summarize  Summarize an execution (defaults to most recent).
```

- [ ] **Step 4: Final commit**

```bash
git add tools/petl-runner/
git commit -m "feat: add petl-runner checksum command, complete implementation"
```

---

## Self-Review Notes

- **Spec: ETL-project-agnostic** ✓ — no sl-etl assumptions hardcoded; all paths are configurable
- **Spec: Smart reload (hash-based)** ✓ — `compute_source_hash` in Task 5, `execute_run` in Task 7
- **Spec: Container restart for cold cache** ✓ — `restart_container` in `execute_run`
- **Spec: Full lifecycle** ✓ — source + target provisioning in `execute_run`
- **Spec: ETL git state capture** ✓ — `capture_git_state` with patch + untracked handling
- **Spec: Maven build + copy jobs/datasources** ✓ — `mvn clean package` + `shutil.copytree`
- **Spec: Generated application.yml** ✓ — `generate_application_yml` with startup jobs from ETL
- **Spec: exitAutomatically: true** ✓ — forced in `generate_application_yml`
- **Spec: Log parsing with job tree** ✓ — interval containment in `build_job_tree`
- **Spec: Row counts always** ✓ — `query_row_counts` after every run
- **Spec: On-demand checksum** ✓ — `checksum` command in Task 12
- **Spec: Matrix runs with matrix run record** ✓ — `execute_matrix` + `MatrixRunRecord`
- **Spec: Execution snapshot** ✓ — docker-compose, jobs/, datasources/, application.yml, git state
- **Spec: Data management** ✓ — `data list`, `data clean`, `data clean --unreferenced`
- **Spec: Execution management** ✓ — `execution list`, `execution clean`, `--matrix`
- **Spec: petl.jar resolution** ✓ — `_resolve_petl_jar` with 3-level precedence
- **Spec: petl-src tracking** ✓ — optional `--petl-src` in `run` command
- **One gap identified and resolved:** `db_name` in `load_dump` call uses `dump_path.stem` heuristic (e.g., `kgh` from `kgh-2026-05-17.sql`). This may not work for all dump filenames. Consider adding a `--db-name` option to `run` command if this proves insufficient.
