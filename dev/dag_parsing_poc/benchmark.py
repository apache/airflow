# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
# /// script
# requires-python = ">=3.10"
# dependencies = ["psutil>=5.8.0"]
# ///
"""Compare both Dag processor commands inside Breeze against identical local definitions."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import resource
import shutil
import signal
import sqlite3
import statistics
import subprocess
import time
from collections import Counter
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory

import psutil

BUNDLE = "parsing-benchmark"
MODES = ("manager", "executor")
# A Dag processor can swallow the first SIGTERM while logging and keep parsing, which
# otherwise stalls a sample until the whole-run timeout. Escalate instead, and record it
# so the affected sample's shutdown and resource data can be excluded.
SHUTDOWN_GRACE_SECONDS = 15.0


class ProcessTree:
    """Remember descendants before their parent exits, including separate process groups."""

    def __init__(self, root: psutil.Process):
        self.root = root
        self.members = {root.pid: root}

    def refresh(self) -> None:
        with suppress(psutil.NoSuchProcess):
            for child in self.root.children(recursive=True):
                self.members[child.pid] = child
        for process in tuple(self.members.values()):
            try:
                if not process.is_running() or process.status() == psutil.STATUS_ZOMBIE:
                    self.members.pop(process.pid, None)
            except psutil.NoSuchProcess:
                self.members.pop(process.pid, None)

    def kill(self) -> int:
        self.refresh()
        alive = []
        for process in self.members.values():
            with suppress(psutil.NoSuchProcess):
                if process.is_running() and process.status() != psutil.STATUS_ZOMBIE:
                    alive.append(process)
                    process.kill()
        _, survivors = psutil.wait_procs(alive, timeout=5)
        if any(process.status() != psutil.STATUS_ZOMBIE for process in survivors):
            raise RuntimeError("Benchmark descendants survived cleanup")
        return len(alive)


def capture_sources(output: Path, revision: str, source_patch: Path) -> dict[str, str]:
    root = Path(__file__).resolve().parents[2]
    patch = source_patch.read_text()
    (output / "source.patch").write_text(patch)
    paths = {path.relative_to(root) for path in Path(__file__).parent.rglob("*.py")}
    for line in patch.splitlines():
        if line.startswith("+++ b/"):
            path = Path(line[6:])
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Source patch contains a path outside the repository")
            if path.suffix == ".py":
                paths.add(path)
    hashes = {}
    for path in sorted(paths):
        source = root / path
        target = output / "sources" / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        hashes[str(path)] = hashlib.sha256(source.read_bytes()).hexdigest()
    write_json(output / "source-state.json", {"revision": revision, "files": hashes})
    return hashes


def verify_sources(hashes: dict[str, str]) -> None:
    root = Path(__file__).resolve().parents[2]
    for path, expected in hashes.items():
        if hashlib.sha256((root / path).read_bytes()).hexdigest() != expected:
            raise RuntimeError(f"Source changed during measurement: {path}")


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n")


def build_environment(home: Path, source: Path, parallelism: int) -> dict[str, str]:
    env = {name: value for name, value in os.environ.items() if not name.startswith(("AIRFLOW_", "_AIRFLOW"))}
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW_CONFIG=str(home / "airflow.cfg"),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__EXECUTOR="LocalExecutor",
        AIRFLOW__CORE__PARALLELISM="7",
        AIRFLOW__CORE__DAG_DISCOVERY_SAFE_MODE="False",
        AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL="0",
        AIRFLOW__DAG_PROCESSOR__PARSING_PROCESSES=str(parallelism),
        AIRFLOW__DAG_PROCESSOR__MIN_FILE_PROCESS_INTERVAL="0",
        AIRFLOW__DAG_PROCESSOR__FILE_PARSING_SORT_MODE="alphabetical",
        AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL="300",
        AIRFLOW__DAG_PROCESSOR__DAG_FILE_PROCESSOR_TIMEOUT="120",
        AIRFLOW__DAG_PROCESSOR__PRINT_STATS_INTERVAL="0",
        AIRFLOW__LOGGING__DAG_PROCESSOR_CHILD_PROCESS_LOG_DIRECTORY=str(home / "parse-logs"),
        AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=json.dumps(
            [
                {
                    "name": BUNDLE,
                    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                    "kwargs": {"path": str(source)},
                }
            ]
        ),
    )
    return env


def write_definitions(source: Path, count: int, tasks: int) -> dict[str, str]:
    source.mkdir()
    hashes = {}
    for index in range(count):
        path = source / f"definition_{index:04}.py"
        code = (
            "from datetime import datetime, timezone\n"
            "from airflow.sdk import DAG\n"
            "from airflow.providers.standard.operators.empty import EmptyOperator\n\n"
            f"with DAG('benchmark_{index:04}', schedule=None, catchup=False, "
            "start_date=datetime(2024, 1, 1, tzinfo=timezone.utc)):\n"
            "    previous = None\n"
            f"    for index in range({tasks}):\n"
            "        current = EmptyOperator(task_id=f'task_{index:03}')\n"
            "        if previous is not None:\n"
            "            previous >> current\n"
            "        previous = current\n"
        )
        path.write_text(code)
        hashes[path.name] = hashlib.sha256(code.encode()).hexdigest()
    return hashes


def initialize_template(home: Path, env: dict[str, str]) -> Path:
    home.mkdir()
    with (home / "migration.log").open("w") as log:
        subprocess.run(
            ["airflow", "db", "migrate"],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
            timeout=180,
            check=True,
        )
    database = home / "airflow.db"
    with sqlite3.connect(database) as connection:
        connection.executescript(
            """
            CREATE TABLE benchmark_acceptance (
                id INTEGER PRIMARY KEY, dag_id TEXT NOT NULL, parsed_at TEXT NOT NULL
            );
            CREATE TRIGGER benchmark_dag_insert AFTER INSERT ON dag
            WHEN NEW.last_parsed_time IS NOT NULL
            BEGIN
                INSERT INTO benchmark_acceptance (dag_id, parsed_at)
                VALUES (NEW.dag_id, NEW.last_parsed_time);
            END;
            CREATE TRIGGER benchmark_dag_update AFTER UPDATE OF last_parsed_time ON dag
            WHEN NEW.last_parsed_time IS NOT NULL AND NEW.last_parsed_time IS NOT OLD.last_parsed_time
            BEGIN
                INSERT INTO benchmark_acceptance (dag_id, parsed_at)
                VALUES (NEW.dag_id, NEW.last_parsed_time);
            END;
            """
        )
    return database


def collect_tree_rss(process: psutil.Process) -> tuple[int, int]:
    try:
        processes = [process, *process.children(recursive=True)]
    except psutil.NoSuchProcess:
        return 0, 0
    rss = 0
    alive = 0
    for item in processes:
        try:
            rss += item.memory_info().rss
            alive += 1
        except psutil.NoSuchProcess:
            pass
    return rss, alive


def run_sample(
    home: Path,
    template: Path,
    source: Path,
    *,
    mode: str,
    definitions: int,
    tasks: int,
    parallelism: int,
    cycles: int,
    timeout: float,
    sample_interval: float,
) -> dict:
    home.mkdir()
    database = home / "airflow.db"
    with sqlite3.connect(template) as template_connection, sqlite3.connect(database) as connection:
        template_connection.backup(connection)
    command = ["airflow", "dag-processor", "--bundle-name", BUNDLE]
    if mode == "executor":
        command.append("--executor-parsing")
    expected_ids = {f"benchmark_{index:04}" for index in range(definitions)}
    counts: Counter[str] = Counter()
    visible_at: dict[str, list[float]] = {dag_id: [] for dag_id in expected_ids}
    last_id = 0
    busy_polls = 0
    peak_rss = peak_processes = 0
    stop_requested_at = None
    escalations = 0
    forced_cleanup = 0
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    started = time.monotonic()
    with (
        (home / "command.log").open("w") as log,
        sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.01) as monitor,
        subprocess.Popen(
            command,
            env=build_environment(home, source, parallelism),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as process,
    ):
        parent = psutil.Process(process.pid)
        tree = ProcessTree(parent)
        try:
            while True:
                tree.refresh()
                returncode = process.poll()
                rss, processes = collect_tree_rss(parent)
                peak_rss = max(peak_rss, rss)
                peak_processes = max(peak_processes, processes)
                try:
                    rows = monitor.execute(
                        "SELECT id, dag_id FROM benchmark_acceptance WHERE id > ? ORDER BY id", (last_id,)
                    ).fetchall()
                except sqlite3.OperationalError as error:
                    if "locked" not in str(error):
                        raise
                    busy_polls += 1
                    rows = []
                observed = time.monotonic() - started
                for row_id, dag_id in rows:
                    counts[dag_id] += 1
                    visible_at[dag_id].append(observed)
                    last_id = row_id
                if stop_requested_at is None and all(counts[dag_id] >= cycles for dag_id in expected_ids):
                    stop_requested_at = observed
                    process.send_signal(signal.SIGTERM)
                elif stop_requested_at is not None and returncode is None:
                    waited = observed - stop_requested_at
                    if escalations == 0 and waited > SHUTDOWN_GRACE_SECONDS:
                        escalations = 1
                        process.send_signal(signal.SIGTERM)
                    elif escalations == 1 and waited > 2 * SHUTDOWN_GRACE_SECONDS:
                        escalations = 2
                        forced_cleanup += tree.kill()
                if returncode is not None:
                    break
                if observed > timeout:
                    raise TimeoutError(f"{mode} exceeded {timeout}s; inspect {home / 'command.log'}")
                time.sleep(sample_interval)
        finally:
            forced_cleanup += tree.kill()
            process.wait()
    elapsed = time.monotonic() - started
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    if process.returncode and stop_requested_at is None:
        raise RuntimeError(f"{mode} exited {process.returncode}; inspect {home / 'command.log'}")
    if set(counts) != expected_ids or any(counts[dag_id] < cycles for dag_id in expected_ids):
        raise RuntimeError(f"Unexpected accepted parse counts: {dict(counts)}")
    with sqlite3.connect(database) as connection:
        serialized = connection.execute("SELECT dag_id, data FROM serialized_dag").fetchall()
        if {row[0] for row in serialized} != expected_ids or len(serialized) != definitions:
            raise RuntimeError("Serialized Dag identities do not match the fixture")
        if any(len(json.loads(row[1])["dag"]["tasks"]) != tasks for row in serialized):
            raise RuntimeError("Serialized task counts do not match the fixture")
        if connection.execute("SELECT count(*) FROM import_error").fetchone()[0]:
            raise RuntimeError("The benchmark produced import errors")
        dispatches = None
        remaining_admissions = {}
        if mode == "executor":
            remaining_admissions = dict(
                connection.execute(
                    "SELECT state, count(*) FROM admissions WHERE state != 'released' GROUP BY state"
                ).fetchall()
            )
            if remaining_admissions.get("submitted"):
                raise RuntimeError("The executor command left unreconciled submitted work")
            if connection.execute("SELECT count(*) FROM attempts WHERE result_json IS NOT NULL").fetchone()[
                0
            ] != sum(counts.values()):
                raise RuntimeError("The executor command is missing result receipts")
            dispatches = connection.execute("SELECT count(*) FROM admissions").fetchone()[0]
    cycle_ends = [max(times[index] for times in visible_at.values()) for index in range(cycles)]
    steady_seconds = cycle_ends[-1] - cycle_ends[0]
    result = {
        "mode": mode,
        "definitions": definitions,
        "tasks_per_definition": tasks,
        "parallelism": parallelism,
        "cycles": cycles,
        "elapsed_seconds": cycle_ends[-1],
        "process_exit_seconds": elapsed,
        "shutdown_seconds": elapsed - cycle_ends[-1],
        "process_exit_code": process.returncode,
        "shutdown_failed": bool(process.returncode or escalations or forced_cleanup),
        "shutdown_escalations": escalations,
        "forced_cleanup_processes": forced_cleanup,
        "first_result_seconds": min(times[0] for times in visible_at.values()),
        "first_cycle_seconds": cycle_ends[0],
        "cycle_completion_seconds": cycle_ends,
        "repeat_definitions_per_second": definitions * (cycles - 1) / steady_seconds if cycles > 1 else None,
        "repeat_acceptances_per_second": (
            sum(
                timestamp > cycle_ends[0] and timestamp <= cycle_ends[-1]
                for times in visible_at.values()
                for timestamp in times
            )
            / steady_seconds
            if cycles > 1
            else None
        ),
        "cpu_seconds": after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
        "peak_tree_rss_mib": peak_rss / 1024**2,
        "peak_processes": peak_processes,
        "metadata_busy_polls": busy_polls,
        "accepted_definitions": sum(counts.values()),
        "extra_accepted_definitions": sum(counts.values()) - definitions * cycles,
        "remaining_admissions": remaining_admissions,
        "executor_dispatches": dispatches,
        "log_bytes": sum(
            path.stat().st_size
            for path in home.rglob("*")
            if path.is_file() and not path.is_symlink() and path.suffix in {".log", ".jsonl"}
        ),
        "database_bytes": database.stat().st_size
        + (Path(str(database) + "-wal").stat().st_size if Path(str(database) + "-wal").exists() else 0),
        "command": command,
        "output": str(home),
    }
    write_json(home / "result.json", result)
    return result


def build_summary(samples: list[dict]) -> list[dict]:
    groups = []
    for definitions, parallelism in sorted({(row["definitions"], row["parallelism"]) for row in samples}):
        for mode in MODES:
            matching = [
                row
                for row in samples
                if (row["definitions"], row["parallelism"], row["mode"]) == (definitions, parallelism, mode)
            ]
            metrics = {}
            for metric in (
                "elapsed_seconds",
                "first_result_seconds",
                "first_cycle_seconds",
                "repeat_definitions_per_second",
                "repeat_acceptances_per_second",
                "cpu_seconds",
                "peak_tree_rss_mib",
            ):
                resources = metric in {"cpu_seconds", "peak_tree_rss_mib"}
                values = [
                    row[metric]
                    for row in matching
                    if row[metric] is not None
                    and (
                        not resources
                        or not (
                            row["shutdown_failed"]
                            or row.get("shutdown_escalations")
                            or row.get("forced_cleanup_processes")
                        )
                    )
                ]
                if values:
                    metrics[metric] = {
                        "median": statistics.median(values),
                        "min": min(values),
                        "max": max(values),
                        "samples": len(values),
                    }
            groups.append(
                {
                    "mode": mode,
                    "definitions": definitions,
                    "parallelism": parallelism,
                    "samples": len(matching),
                    "metrics": metrics,
                }
            )
    return groups


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--revision", required=True, help="Source commit from the host's git rev-parse HEAD")
    parser.add_argument("--source-patch", required=True, type=Path, help="Host git diff --binary HEAD output")
    parser.add_argument("--definitions", nargs="+", type=int, default=[10, 100])
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--tasks", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=900)
    parser.add_argument("--sample-interval", type=float, default=0.1)
    args = parser.parse_args()
    if (
        min(
            *args.definitions,
            args.parallelism,
            args.cycles,
            args.repetitions,
            args.tasks,
            args.timeout,
            args.sample_interval,
        )
        <= 0
    ):
        parser.error("Counts, timeout and sample interval must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    source_hashes = capture_sources(output, args.revision, args.source_patch)
    write_json(
        output / "environment.json",
        {
            "revision": args.revision,
            "driver_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "python": platform.python_version(),
            "logical_cpus": psutil.cpu_count(),
            "cpu_affinity": psutil.Process().cpu_affinity(),
            "memory_mib": psutil.virtual_memory().total / 1024**2,
            "cgroup_cpu_max": (
                Path("/sys/fs/cgroup/cpu.max").read_text().strip()
                if Path("/sys/fs/cgroup/cpu.max").exists()
                else None
            ),
            "config": vars(args) | {"output": str(output), "source_patch": str(args.source_patch)},
            "sample_interval_seconds": args.sample_interval,
            "notes": [
                "Database migration and fixture generation are excluded from timing.",
                "Runtime files use container-local temporary storage; artifact copying is excluded.",
                "Fresh cloned SQLite metadata database for each sample; OS caches are not flushed.",
                "Completion includes discovery, importing and metadata persistence; shutdown is separate.",
                "Both run continuously and receive SIGTERM after the target acceptance count is observed.",
                "CPU and memory include shutdown; extra parses completed during shutdown are recorded.",
                "Nonzero exits after the target is reached are retained as shutdown failures, not successes.",
                "A trigger audits last_parsed_time writes; milestones observe committed rows by polling.",
                "Cycles are per-definition counts, not globally synchronized rounds.",
                "Summed RSS includes shared pages multiple times and is sampled, not physical peak memory.",
                "CPU time includes child processes reaped by the command; the observer is excluded.",
                "Only local Python definitions are measured; no remote placement, tasks or failures.",
                "Executor route batch size is the command default of ten definitions.",
            ],
        },
    )
    with TemporaryDirectory(prefix="airflow-parsing-benchmark-") as scratch:
        workspace = Path(scratch)
        template_home = workspace / "template"
        placeholder_source = workspace / "unused-source"
        placeholder_source.mkdir()
        template = initialize_template(
            template_home, build_environment(template_home, placeholder_source, args.parallelism)
        )
        shutil.copytree(template_home, output / "template")
        samples = []
        for definitions in args.definitions:
            source = workspace / f"definitions-{definitions}"
            write_json(
                output / f"definitions-{definitions}.json", write_definitions(source, definitions, args.tasks)
            )
            shutil.copytree(source, output / source.name)
            for repetition in range(args.repetitions):
                modes = MODES if repetition % 2 == 0 else tuple(reversed(MODES))
                for mode in modes:
                    label = f"definitions-{definitions}-repeat-{repetition + 1}-{mode}"
                    home = workspace / label
                    print(json.dumps({"event": "start", "sample": label}), flush=True)
                    try:
                        sample = run_sample(
                            home,
                            template,
                            source,
                            mode=mode,
                            definitions=definitions,
                            tasks=args.tasks,
                            parallelism=args.parallelism,
                            cycles=args.cycles,
                            timeout=args.timeout,
                            sample_interval=args.sample_interval,
                        )
                    finally:
                        shutil.copytree(home, output / label)
                    verify_sources(source_hashes)
                    sample["output"] = str(output / label)
                    write_json(output / label / "result.json", sample)
                    samples.append(sample | {"repetition": repetition + 1})
                    write_json(output / "samples.json", samples)
                    write_json(output / "summary.json", build_summary(samples))
                    print(json.dumps({"event": "complete", **sample}), flush=True)


if __name__ == "__main__":
    main()
