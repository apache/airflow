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
"""Attach py-spy to the executor parsing prototype once it reaches steady state.

Reuses the benchmark driver's fixture, environment and acceptance triggers so the
profiled run is the same workload the September 25 comparison measured.
"""

from __future__ import annotations

import argparse
import json
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from collections import Counter, defaultdict
from contextlib import suppress
from pathlib import Path
from tempfile import TemporaryDirectory

import psutil

sys.path.insert(0, str(Path(__file__).resolve().parent))

from benchmark import (
    BUNDLE,
    ProcessTree,
    build_environment,
    capture_sources,
    initialize_template,
    verify_sources,
    write_definitions,
    write_json,
)

PY_SPY = "/usr/python/bin/py-spy"
ATTRIBUTION = {
    "supervise_dag_parse": "worker: whole batch",
    "_post_with_retry": "worker: HTTP claim+result",
    "parse_definition": "worker: supervised import",
    "resolve_definition_path": "worker: path check + sha256",
    "compute_source_revision": "worker: sha256 of source",
    "import_sdk_definition": "child: import+serialize",
    "build_sdk_definition": "child: re-read + sha256",
    "check_dag_file_stability": "child: ast stability check",
    "_serialize_dags": "child: serialization",
    "import_definition": "child: user Dag import",
    "accept_result": "api: result acceptance",
    "_persist_result": "api: metadata persistence",
    "update_dag_parsing_results_in_db": "api: Dag metadata write",
    "deepcopy": "api: serialized-Dag deepcopy",
    "claim": "api: claim",
    "avalidated_claims": "api: JWT verify",
    "_open_transaction": "any: sqlite transaction open",
    "step": "host: orchestrator admission",
    "get_sources": "host: full source scan",
    "update_inventory": "host: discovery snapshot",
    "tick": "runner: dispatch tick",
    "heartbeat": "runner: executor heartbeat",
    "_reconcile_returned": "runner: retirement",
}


def find_roles(host: psutil.Process) -> dict[str, list[psutil.Process]]:
    """Identify the API, runner and LocalExecutor worker processes in the host's tree."""

    def cmdline(process: psutil.Process) -> str:
        try:
            return " ".join(process.cmdline())
        except psutil.Error:
            return ""

    # multiprocessing's spawn context adds a resource_tracker child that is not part of the design.
    children = [child for child in host.children() if "resource_tracker" not in cmdline(child)]
    workers = [
        process for child in children for process in child.children() if "LocalExecutor" in cmdline(process)
    ]
    runner = None
    if workers:
        parents = {worker.ppid() for worker in workers}
        runner = next((child for child in children if child.pid in parents), None)
    api = next((child for child in children if runner is None or child.pid != runner.pid), None)
    roles: dict[str, list[psutil.Process]] = {"host": [host]}
    if api is not None:
        roles["api"] = [api]
    if runner is not None:
        roles["runner"] = [runner]
    if workers:
        roles["workers"] = workers
    return roles


def wait_for_steady_state(
    database: Path, expected: set[str], cycles: int, timeout: float, tree: ProcessTree
) -> float:
    """Block until every definition has been accepted ``cycles`` times."""
    counts: Counter[str] = Counter()
    last_id = 0
    deadline = time.monotonic() + timeout
    started = time.monotonic()
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=0.5) as monitor:
        while time.monotonic() < deadline:
            tree.refresh()
            if not tree.root.is_running():
                raise RuntimeError("Dag processor exited during warmup")
            rows = monitor.execute(
                "SELECT id, dag_id FROM benchmark_acceptance WHERE id > ? ORDER BY id", (last_id,)
            ).fetchall()
            for row_id, dag_id in rows:
                counts[dag_id] += 1
                last_id = row_id
            if all(counts[dag_id] >= cycles for dag_id in expected):
                return time.monotonic() - started
            time.sleep(0.02)
    raise TimeoutError(f"Steady state not reached; {len(counts)}/{len(expected)} definitions seen")


def count_acceptances(database: Path) -> int:
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1) as monitor:
        return monitor.execute("SELECT count(*) FROM benchmark_acceptance").fetchone()[0]


def start_recording(role: str, process: psutil.Process, output: Path, duration: int, follow: bool) -> dict:
    """Start one py-spy recorder; raw format gives folded stacks we can aggregate."""
    target = output / f"{role}-{process.pid}.folded"
    command = [
        PY_SPY,
        "record",
        "--pid",
        str(process.pid),
        "--duration",
        str(duration),
        "--rate",
        "100",
        "--format",
        "raw",
        "--idle",
        "--output",
        str(target),
    ]
    if follow:
        command.append("--subprocesses")
    log = (output / f"{role}-{process.pid}.pyspy.log").open("w")
    return {
        "role": role,
        "pid": process.pid,
        "path": target,
        "log": log,
        "process": subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT),
    }


def summarize(path: Path) -> dict:
    """Count stacks separately for the target process and its short-lived descendants."""
    if not path.exists() or not path.stat().st_size:
        raise RuntimeError(f"Empty profile recording: {path}")
    totals: Counter[str] = Counter()
    leaves: dict[str, Counter[str]] = defaultdict(Counter)
    inclusive: dict[str, Counter[str]] = defaultdict(Counter)
    for line in path.read_text().splitlines():
        stack, _, count = line.rpartition(" ")
        if not count.isdigit():
            continue
        weight = int(count)
        frames = stack.split(";")
        population = "descendants" if sum(frame.startswith("process ") for frame in frames) > 1 else "target"
        totals[population] += weight
        leaf = frames[-1]
        leaves[population][leaf] += weight
        seen = set()
        for frame in frames:
            name = frame.split(" ", 1)[0]
            label = ATTRIBUTION.get(name)
            if label is not None and label not in seen:
                seen.add(label)
                inclusive[population][label] += weight
    return {
        "samples": sum(totals.values()),
        "populations": {
            name: {
                "samples": total,
                "inclusive_samples": inclusive[name].most_common(),
                "top_leaves": leaves[name].most_common(20),
            }
            for name, total in totals.items()
        },
        "note": "Stack samples include waiting threads. Counts are not CPU seconds or process wall time.",
    }


def run_profile(args, output: Path, root: Path) -> int:
    source = root / f"definitions-{args.definitions}"
    write_definitions(source, args.definitions, args.tasks)

    template_home = root / "template"
    template_env = build_environment(template_home, source, args.parallelism)
    template = initialize_template(template_home, template_env)

    home = root / "sample"
    home.mkdir()
    database = home / "airflow.db"
    with sqlite3.connect(template) as source_db, sqlite3.connect(database) as target_db:
        source_db.backup(target_db)

    command = ["airflow", "dag-processor", "--bundle-name", BUNDLE, "--executor-parsing"]
    expected = {f"benchmark_{index:04}" for index in range(args.definitions)}
    recorders: list[dict] = []
    report: dict = {
        "definitions": args.definitions,
        "tasks_per_definition": args.tasks,
        "parallelism": args.parallelism,
        "warmup_cycles": args.warmup_cycles,
        "profile_duration_seconds": args.duration,
        "command": " ".join(command),
    }

    with (
        (home / "command.log").open("w") as log,
        subprocess.Popen(
            command,
            env=build_environment(home, source, args.parallelism),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        ) as process,
    ):
        host = psutil.Process(process.pid)
        tree = ProcessTree(host)
        try:
            report["steady_state_seconds"] = wait_for_steady_state(
                database, expected, args.warmup_cycles, args.startup_timeout, tree
            )
            roles = find_roles(host)
            report["roles"] = {
                role: [{"pid": item.pid, "cmdline": " ".join(item.cmdline())[:120]} for item in processes]
                for role, processes in roles.items()
            }
            missing = {"api", "runner", "workers"} - roles.keys()
            if missing:
                raise RuntimeError(f"Could not identify prototype processes: {sorted(missing)}")
            if len(roles["workers"]) != args.parallelism:
                raise RuntimeError("Worker population does not match configured parsing slots")

            accepted_before = count_acceptances(database)
            profile_started = time.monotonic()
            targets = [item for processes in roles.values() for item in processes]
            cpu_before = {item.pid: item.cpu_times() for item in targets}
            for role, processes in roles.items():
                for item in processes:
                    # Following subprocesses captures each worker's forked parse children.
                    recorders.append(
                        start_recording(role, item, output, args.duration, follow=role == "workers")
                    )
            while any(recorder["process"].poll() is None for recorder in recorders):
                tree.refresh()
                if time.monotonic() - profile_started > args.duration + 30:
                    raise TimeoutError("Profiler did not finish")
                time.sleep(0.02)
            if any(recorder["process"].returncode for recorder in recorders):
                raise RuntimeError("Profiler failed; inspect recorder logs")
            report["profile_wall_seconds"] = time.monotonic() - profile_started
            report["accepted_during_profile"] = count_acceptances(database) - accepted_before
            report["cpu_seconds_by_pid"] = {}
            for item in targets:
                after = item.cpu_times()
                before = cpu_before[item.pid]
                report["cpu_seconds_by_pid"][item.pid] = (
                    after.user + after.system - before.user - before.system
                )
            report["cpu_note"] = "CPU deltas cover named processes only; importer child CPU is not included."
        finally:
            for recorder in recorders:
                if recorder["process"].poll() is None:
                    recorder["process"].kill()
                recorder["process"].wait()
                recorder["log"].close()
            with suppress(ProcessLookupError):
                process.send_signal(signal.SIGTERM)
            try:
                deadline = time.monotonic() + 15
                while process.poll() is None and time.monotonic() < deadline:
                    tree.refresh()
                    time.sleep(0.02)
            finally:
                report["forced_cleanup_processes"] = tree.kill()
                process.wait()

    per_role: dict[str, dict] = defaultdict(lambda: {"samples": 0, "files": []})
    for recorder in recorders:
        summary = summarize(recorder["path"])
        report.setdefault("profiles", []).append(
            {"role": recorder["role"], "pid": recorder["pid"], "file": recorder["path"].name, **summary}
        )
        per_role[recorder["role"]]["samples"] += summary.get("samples", 0)
        per_role[recorder["role"]]["files"].append(recorder["path"].name)
    report["samples_by_role"] = dict(per_role)
    write_json(output / "profile-summary.json", report)

    print(json.dumps({key: report[key] for key in report if key != "profiles"}, indent=2))
    for entry in report.get("profiles", []):
        print(json.dumps(entry))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--source-patch", required=True, type=Path)
    parser.add_argument("--definitions", type=int, default=100)
    parser.add_argument("--tasks", type=int, default=20)
    parser.add_argument("--parallelism", type=int, default=2)
    parser.add_argument("--warmup-cycles", type=int, default=1)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--startup-timeout", type=float, default=180)
    args = parser.parse_args()
    if (
        min(
            args.definitions,
            args.tasks,
            args.parallelism,
            args.warmup_cycles,
            args.duration,
            args.startup_timeout,
        )
        <= 0
    ):
        parser.error("Counts and durations must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    hashes = capture_sources(output, args.revision, args.source_patch)
    with TemporaryDirectory(prefix="airflow-parsing-profile-") as scratch:
        root = Path(scratch)
        try:
            return run_profile(args, output, root)
        finally:
            shutil.copytree(root, output / "workspace")
            verify_sources(hashes)


if __name__ == "__main__":
    raise SystemExit(main())
