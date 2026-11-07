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

from __future__ import annotations

import json
import subprocess
import sys
from unittest import mock

import pytest

from airflow.dag_processing.health import check_processor_health, write_processor_health

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def reset_health_write_warnings(monkeypatch):
    monkeypatch.setattr("airflow.dag_processing.health._next_health_write_warnings", {})


@pytest.mark.parametrize(
    ("loop_age", "api_age", "readiness", "healthy"),
    [
        (0, 0, False, True),
        (31, 0, False, False),
        (0, 31, False, True),
        (0, 31, True, False),
        (0, 0, True, True),
        (-1, 0, False, False),
    ],
)
@mock.patch("airflow.dag_processing.health.monotonic", autospec=True)
def test_local_health_requires_a_fresh_loop(clock, tmp_path, loop_age, api_age, readiness, healthy):
    with conf_vars(
        {
            ("dag_processor", "health_check_file"): str(tmp_path / "health.json"),
            ("dag_processor", "health_check_threshold"): "30",
        }
    ):
        clock.return_value = 100 - loop_age
        write_processor_health(100 - api_age)
        clock.return_value = 100
        if healthy:
            check_processor_health(readiness=readiness)
        else:
            field, age = ("loop", loop_age) if not 0 <= loop_age <= 30 else ("api", api_age)
            with pytest.raises(
                SystemExit,
                match=rf"Processor {field} heartbeat age {age:.1f}s.*health_check_threshold",
            ):
                check_processor_health(readiness=readiness)


@pytest.mark.parametrize("operation", ["mkdir", "write_text", "replace"])
@mock.patch("airflow.dag_processing.health.Path.replace", autospec=True)
@mock.patch("airflow.dag_processing.health.Path.write_text", autospec=True)
@mock.patch("airflow.dag_processing.health.Path.mkdir", autospec=True)
def test_health_write_failure_is_logged_without_stopping_the_processor(
    mkdir, write_text, replace, tmp_path, caplog, operation
):
    path = tmp_path / "health.json"
    operations = {"mkdir": mkdir, "write_text": write_text, "replace": replace}
    operations[operation].side_effect = OSError("Health file is not writable")

    with conf_vars({("dag_processor", "health_check_file"): str(path)}):
        write_processor_health(0)

        with pytest.raises(SystemExit, match="Dag processor is not healthy: .*health.json"):
            check_processor_health()

    operations[operation].assert_called_once()
    assert {
        "event": "Unable to write [dag_processor] health_check_file",
        "path": str(path),
        "error": "Health file is not writable",
    } in caplog


@mock.patch("airflow.dag_processing.health.monotonic", autospec=True)
@mock.patch("airflow.dag_processing.health.Path.write_text", autospec=True)
def test_health_write_warnings_are_throttled_without_delaying_writes(write_text, clock, tmp_path, caplog):
    write_text.side_effect = PermissionError("Health file is not writable")
    with conf_vars({("dag_processor", "health_check_file"): str(tmp_path / "health.json")}):
        for tick, should_warn in ((100, True), (101, False), (159, False), (160, True)):
            clock.return_value = tick
            caplog.clear()
            write_processor_health(0)
            assert ("Unable to write [dag_processor] health_check_file" in caplog) == should_warn

    assert write_text.call_count == 4


@mock.patch("airflow.dag_processing.health.monotonic", autospec=True)
@mock.patch("airflow.dag_processing.health.Path.mkdir", autospec=True)
def test_health_write_recovery_resets_the_warning_interval(mkdir, clock, tmp_path, caplog):
    path = tmp_path / "health.json"
    error = PermissionError("Health file is not writable")
    mkdir.side_effect = [error, None, error]
    with conf_vars({("dag_processor", "health_check_file"): str(path)}):
        clock.return_value = 100
        write_processor_health(0)
        assert "Unable to write [dag_processor] health_check_file" in caplog

        caplog.clear()
        clock.return_value = 101
        write_processor_health(101)
        check_processor_health(readiness=True)
        assert "Unable to write [dag_processor] health_check_file" not in caplog

        clock.return_value = 102
        write_processor_health(101)
        assert "Unable to write [dag_processor] health_check_file" in caplog

    assert mkdir.call_count == 3


@mock.patch("airflow.dag_processing.health.monotonic", autospec=True, return_value=100)
@mock.patch("airflow.dag_processing.health.Path.write_text", autospec=True)
def test_health_write_warnings_are_scoped_to_each_path(write_text, clock, tmp_path, caplog):
    write_text.side_effect = PermissionError("Health file is not writable")
    for filename, should_warn in (("first.json", True), ("second.json", True), ("first.json", False)):
        path = tmp_path / filename
        caplog.clear()
        with conf_vars({("dag_processor", "health_check_file"): str(path)}):
            write_processor_health(0)
        assert (
            {"event": "Unable to write [dag_processor] health_check_file", "path": str(path)} in caplog
        ) == should_warn


@pytest.mark.parametrize(
    ("contents", "cause", "detail"),
    [
        (None, FileNotFoundError, "health.json"),
        ("broken", json.JSONDecodeError, "Expecting value"),
        ("{}", KeyError, "pid"),
    ],
)
def test_missing_or_invalid_health_file_fails_closed(tmp_path, contents, cause, detail):
    path = tmp_path / "health.json"
    if contents is not None:
        path.write_text(contents)
    with (
        conf_vars({("dag_processor", "health_check_file"): str(path)}),
        pytest.raises(SystemExit, match=f"Dag processor is not healthy: .*{detail}") as raised,
    ):
        check_processor_health()
    assert isinstance(raised.value.__cause__, cause)


def test_dead_processor_fails_health_with_a_fresh_heartbeat(tmp_path):
    path = tmp_path / "health.json"
    with subprocess.Popen([sys.executable, "-c", "pass"]) as process:
        process.wait(timeout=10)
    with conf_vars({("dag_processor", "health_check_file"): str(path)}):
        write_processor_health(0)
        record = json.loads(path.read_text())
        record["pid"] = process.pid
        path.write_text(json.dumps(record))
        with pytest.raises(SystemExit, match="Dag processor is not healthy: .*No such process") as raised:
            check_processor_health()
    assert isinstance(raised.value.__cause__, ProcessLookupError)


@mock.patch("airflow.dag_processing.health.os.kill", autospec=True)
def test_inaccessible_processor_reports_the_probe_error(kill, tmp_path):
    kill.side_effect = PermissionError("Operation not permitted")
    with conf_vars({("dag_processor", "health_check_file"): str(tmp_path / "health.json")}):
        write_processor_health(0)
        with pytest.raises(SystemExit, match="Dag processor is not healthy: Operation not permitted"):
            check_processor_health()
