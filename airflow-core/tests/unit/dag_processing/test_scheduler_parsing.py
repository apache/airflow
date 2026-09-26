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
import sqlite3
import time
from unittest import mock

import pytest

from airflow.dag_processing.orchestrator import DiscoveredDefinition
from airflow.dag_processing.scheduler_parsing import (
    ParsingStepBudgetExceeded,
    SchedulerParsingConfig,
    SchedulerParsingHost,
)
from airflow.executors.workloads import BundleInfo

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def config_path(tmp_path):
    path = tmp_path / "parsing.json"
    path.write_text(json.dumps({"route": "scheduler-parsing", "bundle": "poc", "capacity": 1}))
    return path


@pytest.fixture
def host(config_path, tmp_path):
    database = tmp_path / "metadata.sqlite"
    database.touch()
    with conf_vars(
        {("database", "sql_alchemy_conn"): f"sqlite:///{database}", ("core", "multi_team"): "False"}
    ):
        host = SchedulerParsingHost(str(config_path))
        host.start()
        try:
            yield host
        finally:
            host.close()


def test_registered_inventory_admission_preserves_metadata_authority(host):
    host.orchestrator.update_inventory(
        BundleInfo(name="poc", version="v1"),
        [DiscoveredDefinition(relative_path="not-on-scheduler.py", source_revision="revision")],
    )
    tick = host.tick()
    assert tick.status == "admitted"
    with host.store._open_transaction() as connection:
        current = connection.execute("SELECT * FROM current_definitions").fetchone()
    assert current["workload_id"] == tick.workload_id
    assert current["relative_path"] == "not-on-scheduler.py"
    assert host.tick().status == "idle"
    assert len(host.store.get_admissions(host.config.route)) == 1
    assert host.store.deadline is None


def test_busy_parsing_transaction_defers_without_waiting(host):
    with sqlite3.connect(host.path, timeout=0) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        tick = host.tick()
        assert time.monotonic() - started < 1
        assert tick.status == "deferred"
    assert host.tick().status == "idle"


@mock.patch("airflow.dag_processing.scheduler_parsing.time.monotonic", autospec=True, return_value=0)
@pytest.mark.parametrize("expiry", ["before_open", "before_commit", "during_query"])
def test_budget_expiry_rolls_back_admission_work(monotonic, host, expiry):
    store = host.store
    store.deadline = 10
    if expiry == "before_open":
        monotonic.return_value = 11
    expected = sqlite3.OperationalError if expiry == "during_query" else ParsingStepBudgetExceeded

    def write_then_expire():
        with store._open_transaction() as connection:
            connection.execute("INSERT INTO workloads VALUES ('uncommitted', '{}')")
            monotonic.return_value = 11
            if expiry == "during_query":
                connection.execute(
                    "WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i+1 FROM n WHERE i<1000000) "
                    "SELECT sum(i) FROM n"
                ).fetchone()

    try:
        with pytest.raises(expected):
            write_then_expire()
    finally:
        store.deadline = None
    with store._open_transaction() as connection:
        assert connection.execute("SELECT count(*) FROM workloads").fetchone()[0] == 0


@pytest.mark.parametrize(
    ("error", "deferred"),
    [
        (ParsingStepBudgetExceeded("budget"), True),
        (sqlite3.OperationalError("interrupted"), True),
        (sqlite3.OperationalError("database table is locked"), True),
        (sqlite3.OperationalError("no such table: parse_sources"), False),
    ],
)
@mock.patch("airflow.dag_processing.scheduler_parsing.stats", autospec=True)
def test_tick_reports_deferred_and_unexpected_failures(stats, host, error, deferred, monkeypatch):
    monkeypatch.setattr(
        host.orchestrator, "step", mock.create_autospec(host.orchestrator.step, side_effect=error)
    )
    if deferred:
        assert host.tick().status == "deferred"
    else:
        with pytest.raises(sqlite3.OperationalError, match="no such table"):
            host.tick()
    assert host.store.deadline is None
    stats.timing.assert_called_once()
    stats.incr.assert_called_once_with(f"scheduler.parsing_step_{'deferred' if deferred else 'failed'}")


def test_host_excludes_other_parsing_hosts_and_releases_on_close(host, config_path):
    other = SchedulerParsingHost(str(config_path))
    with pytest.raises(RuntimeError, match="Another executor parsing host"):
        other.start()
    host.close()
    other.start()
    try:
        with pytest.raises(RuntimeError, match="already started"):
            other.start()
    finally:
        other.close()
    with pytest.raises(RuntimeError, match="before ticking"):
        other.tick()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("route", "default"),
        ("route", "celery"),
        ("route", "task route"),
        ("capacity", 0),
        ("capacity", 33),
        ("batch_size", 11),
        ("interval", 0),
        ("step_budget", 1),
        ("step_budget", float("nan")),
        ("parse_interval", float("inf")),
        ("bundle_root", "/dag-source"),
    ],
)
def test_config_rejects_unbounded_or_source_settings(field, value):
    with pytest.raises(ValueError, match=field):
        SchedulerParsingConfig.model_validate({"route": "parsing", "bundle": "poc", field: value})


@pytest.mark.parametrize(
    "url", ["sqlite:///:memory:", "postgresql://localhost/airflow", "sqlite:///db?mode=ro"]
)
def test_host_requires_file_backed_sqlite(url, config_path):
    with conf_vars({("database", "sql_alchemy_conn"): url}):
        with pytest.raises(ValueError, match="file-backed SQLite"):
            SchedulerParsingHost(str(config_path))


@conf_vars({("core", "multi_team"): "True"})
def test_host_rejects_multi_team(config_path):
    with pytest.raises(ValueError, match="single-team"):
        SchedulerParsingHost(str(config_path))


def test_host_bounds_configuration_file(config_path):
    config_path.write_text(" " * 8193)
    with pytest.raises(ValueError, match="exceeds 8 KiB"):
        SchedulerParsingHost(str(config_path))
