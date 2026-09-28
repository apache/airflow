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
# ruff: noqa: S101
from __future__ import annotations

import json
import subprocess
import sys
import time
from unittest import mock

import psutil
import pytest

from dev.dag_parsing_poc import benchmark, profile_steady_state


@pytest.mark.parametrize("fault", ["shutdown_failed", "shutdown_escalations", "forced_cleanup_processes"])
def test_summary_excludes_failed_shutdown_resources_but_keeps_completion(fault):
    sample = {
        "mode": "manager",
        "definitions": 10,
        "parallelism": 2,
        "shutdown_failed": False,
        "elapsed_seconds": 4,
        "first_result_seconds": 1,
        "first_cycle_seconds": 2,
        "repeat_definitions_per_second": 10,
        "repeat_acceptances_per_second": 12,
        "cpu_seconds": 3,
        "peak_tree_rss_mib": 100,
    }
    failed = sample | {fault: 1, "elapsed_seconds": 6, "cpu_seconds": 999, "peak_tree_rss_mib": 999}
    group = benchmark.build_summary([sample, failed])[0]
    assert group["metrics"]["elapsed_seconds"]["median"] == 5
    assert group["metrics"]["elapsed_seconds"]["samples"] == 2
    assert group["metrics"]["cpu_seconds"] == {"median": 3, "min": 3, "max": 3, "samples": 1}
    assert group["metrics"]["peak_tree_rss_mib"]["median"] == 100
    failed_group = benchmark.build_summary([failed])[0]
    assert "cpu_seconds" not in failed_group["metrics"]


def test_cleanup_remembers_importer_after_parent_exit(tmp_path):
    pid_file = tmp_path / "child.pid"
    child_code = "import time; time.sleep(60)"
    parent_code = (
        "import pathlib,subprocess,sys,time;"
        f"child=subprocess.Popen([sys.executable,'-c',{child_code!r}],start_new_session=True);"
        f"pathlib.Path({str(pid_file)!r}).write_text(str(child.pid));time.sleep(60)"
    )
    with subprocess.Popen([sys.executable, "-c", parent_code], start_new_session=True) as process:
        tree = benchmark.ProcessTree(psutil.Process(process.pid))
        try:
            deadline = time.monotonic() + 10
            while not pid_file.exists() and time.monotonic() < deadline:
                time.sleep(0.01)
            child = psutil.Process(int(pid_file.read_text()))
            tree.refresh()
            process.kill()
            process.wait(timeout=5)
            assert tree.kill() >= 1
            assert not child.is_running() or child.status() == psutil.STATUS_ZOMBIE
            assert tree.kill() == 0
        finally:
            tree.kill()


def test_source_snapshot_detects_edits_and_saves_untracked_drivers(tmp_path, monkeypatch):
    source = tmp_path / "repo"
    script = source / "dev" / "dag_parsing_poc" / "benchmark.py"
    script.parent.mkdir(parents=True)
    script.write_text("value = 1\n")
    core = source / "core.py"
    core.write_text("value = 2\n")
    patch = tmp_path / "source.patch"
    patch.write_text("+++ b/core.py\n")
    output = tmp_path / "output"
    output.mkdir()
    monkeypatch.setattr(benchmark, "__file__", str(script))
    hashes = benchmark.capture_sources(output, "base", patch)
    assert (output / "sources" / "dev/dag_parsing_poc/benchmark.py").read_text() == script.read_text()
    assert json.loads((output / "source-state.json").read_text())["revision"] == "base"
    benchmark.verify_sources(hashes)
    core.write_text("value = 3\n")
    with pytest.raises(RuntimeError, match="Source changed"):
        benchmark.verify_sources(hashes)
    patch.write_text("+++ b/../outside.py\n")
    with pytest.raises(ValueError, match="outside"):
        benchmark.capture_sources(output, "base", patch)


def test_profile_counts_target_and_importers_without_inventing_cpu_time(tmp_path):
    recording = tmp_path / "sample.folded"
    recording.write_text(
        "process 1:worker;parse_definition (worker.py:1);select (selectors.py:2) 100\n"
        "process 1:worker;process 2:child;_serialize_dags (processor.py:1) 20\n"
        "process 1:worker;_post_with_retry (worker.py:3);read (httpcore.py:4) 50\n"
        "invalid line\n"
    )
    result = profile_steady_state.summarize(recording)
    assert result["populations"]["target"]["samples"] == 150
    assert result["populations"]["descendants"]["samples"] == 20
    assert dict(result["populations"]["target"]["inclusive_samples"])["worker: HTTP claim+result"] == 50
    assert "active_pct" not in result
    recording.write_text("")
    with pytest.raises(RuntimeError, match="Empty profile"):
        profile_steady_state.summarize(recording)


def test_profile_roles_exclude_importers_with_inherited_worker_title():
    host, api, runner, worker, importer, tracker = [
        mock.create_autospec(psutil.Process, instance=True) for _ in range(6)
    ]
    for index, process in enumerate((host, api, runner, worker, importer, tracker)):
        process.pid = index + 1
    host.children.return_value = [api, runner, tracker]
    api.cmdline.return_value = ["api"]
    api.children.return_value = []
    tracker.cmdline.return_value = ["resource_tracker"]
    runner.cmdline.return_value = ["runner"]
    runner.children.return_value = [worker]
    worker.cmdline.return_value = ["LocalExecutor"]
    worker.ppid.return_value = runner.pid
    worker.children.return_value = [importer]
    importer.cmdline.return_value = ["LocalExecutor"]
    roles = profile_steady_state.find_roles(host)
    assert roles == {"host": [host], "api": [api], "runner": [runner], "workers": [worker]}
