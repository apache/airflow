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
import re
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from threading import Barrier
from typing import TYPE_CHECKING

import pytest
import time_machine
from sqlalchemy import delete, event, func, select

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.jobs.asset_partition_warnings import AssetPartitionWaitMonitor
from airflow.models.asset import AssetPartitionDagRun
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import count_queries
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags

if TYPE_CHECKING:
    from collections.abc import Iterator
    from datetime import datetime

    from sqlalchemy.orm import Session

pytestmark = pytest.mark.db_test

NOW = timezone.datetime(2026, 11, 12, 12)
TIMEOUT_OPTION = ("scheduler", "pending_partitioned_dag_run_timeout")
WARNING_TYPE = DagWarningType.ASSET_PARTITION_PENDING


@pytest.fixture(autouse=True)
def clean_monitor_data(testing_dag_bundle):
    def clean():
        with create_session(scoped=False) as session:
            session.execute(delete(AssetPartitionDagRun))
        clear_db_dags()

    clean()
    yield
    clean()


def _add_dag(session: Session, dag_id: str = "monitored", **overrides) -> DagModel:
    values = {
        "dag_id": dag_id,
        "bundle_name": "testing",
        "is_paused": False,
        "is_draining": False,
        "is_stale": False,
        "timetable_partitioned": True,
        "asset_expression": {"asset": {"name": "input", "uri": "input"}},
    }
    dag = DagModel(**(values | overrides))
    session.add(dag)
    session.flush()
    return dag


def _add_pending(
    session: Session,
    dag_id: str = "monitored",
    *,
    age: timedelta = timedelta(hours=2),
    partition_key: str = "partition",
) -> AssetPartitionDagRun:
    row = AssetPartitionDagRun(
        target_dag_id=dag_id,
        partition_key=partition_key,
        created_at=NOW - age,
        updated_at=NOW,
        rollup_fingerprint=None,
    )
    session.add(row)
    return row


def _get_warnings() -> dict[tuple[str, str], tuple[str, datetime]]:
    with create_session(scoped=False) as session:
        return {
            (dag_id, warning_type): (message, timestamp)
            for dag_id, warning_type, message, timestamp in session.execute(
                select(DagWarning.dag_id, DagWarning.warning_type, DagWarning.message, DagWarning.timestamp)
            )
        }


@contextmanager
def _capture_statements() -> Iterator[list[str]]:
    statements: list[str] = []

    def capture(_connection, _cursor, statement, _parameters, _context, _executemany):
        statements.append(statement)

    event.listen(settings.engine, "before_cursor_execute", capture)
    try:
        yield statements
    finally:
        event.remove(settings.engine, "before_cursor_execute", capture)


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
@pytest.mark.parametrize(
    ("age", "warned"),
    [
        pytest.param(timedelta(seconds=-1), False, id="future-created-at"),
        pytest.param(timedelta(seconds=3599), False, id="below-threshold"),
        pytest.param(timedelta(seconds=3600), False, id="exact-threshold"),
        pytest.param(timedelta(seconds=3601), True, id="above-threshold"),
    ],
)
def test_warns_only_after_pending_wait_exceeds_threshold(age, warned):
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session, age=age)

    AssetPartitionWaitMonitor().check()

    warnings = _get_warnings()
    assert (("monitored", WARNING_TYPE) in warnings) is warned
    with create_session(scoped=False) as session:
        assert session.scalar(select(func.count()).select_from(DagRun)) == 0
        assert session.scalar(select(AssetPartitionDagRun.created_dag_run_id)) is None


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
def test_reports_pending_rows_and_oldest_creation_without_distinct_key_deduplication():
    oldest = NOW - timedelta(hours=2)
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
        _add_pending(session, age=timedelta(minutes=30))
        _add_pending(session, age=timedelta(minutes=5))

    AssetPartitionWaitMonitor().check()

    message, _ = _get_warnings()["monitored", WARNING_TYPE]
    assert re.search(r"\b3\b", message)
    assert oldest.isoformat() in message
    assert "3600" in message


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600, "missing": 3600}'})
def test_does_not_infer_a_missing_partition_or_monitor_unconfigured_dags():
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_dag(session, "unconfigured")
        _add_pending(session, "unconfigured", age=timedelta(days=30))

    AssetPartitionWaitMonitor().check()

    assert _get_warnings() == {}


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
@pytest.mark.parametrize(
    "reason",
    ["completed", "deleted", "paused", "draining", "stale", "nonpartitioned"],
)
def test_clears_warning_when_pending_wait_no_longer_needs_monitoring(reason):
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
        session.add(DagWarning("monitored", DagWarningType.NONEXISTENT_POOL, "Unrelated warning"))
    monitor = AssetPartitionWaitMonitor()
    monitor.check()
    assert ("monitored", WARNING_TYPE) in _get_warnings()

    with create_session(scoped=False) as session:
        if reason == "completed":
            run = DagRun(
                dag_id="monitored",
                run_id="asset_triggered__test",
                run_type=DagRunType.ASSET_TRIGGERED,
                state=DagRunState.QUEUED,
                run_after=NOW,
            )
            session.add(run)
            session.flush()
            session.scalar(select(AssetPartitionDagRun)).created_dag_run_id = run.id
        elif reason == "deleted":
            session.execute(delete(AssetPartitionDagRun))
        else:
            field, value = {
                "paused": ("is_paused", True),
                "draining": ("is_draining", True),
                "stale": ("is_stale", True),
                "nonpartitioned": ("timetable_partitioned", False),
            }[reason]
            setattr(session.get(DagModel, "monitored"), field, value)

    monitor.check()

    warnings = _get_warnings()
    assert set(warnings) == {("monitored", DagWarningType.NONEXISTENT_POOL)}
    assert warnings["monitored", DagWarningType.NONEXISTENT_POOL][0] == "Unrelated warning"


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
@pytest.mark.parametrize("field", ["is_paused", "is_draining", "is_stale"])
def test_warns_after_reactivation_using_original_pending_creation_time(field):
    with create_session(scoped=False) as session:
        _add_dag(session, **{field: True})
        _add_pending(session)
    monitor = AssetPartitionWaitMonitor()
    monitor.check()
    assert _get_warnings() == {}

    with create_session(scoped=False) as session:
        setattr(session.get(DagModel, "monitored"), field, False)
    monitor.check()

    assert ("monitored", WARNING_TYPE) in _get_warnings()


@time_machine.travel(NOW, tick=False)
def test_clears_removed_policy_after_monitor_restart():
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
    with conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'}):
        AssetPartitionWaitMonitor().check()
    assert ("monitored", WARNING_TYPE) in _get_warnings()

    with conf_vars({TIMEOUT_OPTION: "{}"}):
        AssetPartitionWaitMonitor().check()

    assert _get_warnings() == {}


@time_machine.travel(NOW, tick=False)
def test_updated_threshold_reconciles_existing_warning_without_restarting_monitor():
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
    monitor = AssetPartitionWaitMonitor()
    with conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'}):
        monitor.check()
    assert ("monitored", WARNING_TYPE) in _get_warnings()

    with conf_vars({TIMEOUT_OPTION: '{"monitored": 14400}'}):
        monitor.check()
    assert _get_warnings() == {}

    with conf_vars({TIMEOUT_OPTION: '{"monitored": 0.5}'}):
        monitor.check()
    assert ("monitored", WARNING_TYPE) in _get_warnings()


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
def test_counts_all_pending_rows_beyond_scheduler_evaluation_window():
    with create_session(scoped=False) as session:
        _add_dag(session, "earlier-unconfigured")
        _add_dag(session)
        for _ in range(500):
            _add_pending(session, "earlier-unconfigured", age=timedelta(days=30))
        for _ in range(501):
            _add_pending(session)

    AssetPartitionWaitMonitor().check()

    warnings = _get_warnings()
    assert set(warnings) == {("monitored", WARNING_TYPE)}
    assert re.search(r"\b501\b", warnings["monitored", WARNING_TYPE][0])


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
def test_unchanged_warning_keeps_timestamp_and_does_not_write_or_log_again(caplog):
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
    monitor = AssetPartitionWaitMonitor()
    monitor.check()
    first = _get_warnings()
    caplog.clear()

    with time_machine.travel(NOW + timedelta(minutes=1), tick=False), _capture_statements() as statements:
        monitor.check()

    assert _get_warnings() == first
    assert not any(
        statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE"} for statement in statements
    )
    assert not list(caplog)


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
def test_refreshes_existing_warning_when_pending_rows_change():
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
        session.add(DagWarning("monitored", WARNING_TYPE, "Previous observation", timestamp=NOW))
    monitor = AssetPartitionWaitMonitor()
    monitor.check()
    before, _ = _get_warnings()["monitored", WARNING_TYPE]
    assert before != "Previous observation"

    with create_session(scoped=False) as session:
        _add_pending(session)
    monitor.check()

    warnings = _get_warnings()
    assert len(warnings) == 1
    assert re.search(r"\b2\b", warnings["monitored", WARNING_TYPE][0])
    assert warnings["monitored", WARNING_TYPE][0] != before


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"first": 3600, "second": 1800}'})
def test_refreshes_multiple_existing_warnings_with_each_dags_own_observation():
    with create_session(scoped=False) as session:
        for dag_id, count in (("first", 1), ("second", 3)):
            _add_dag(session, dag_id)
            for _ in range(count):
                _add_pending(session, dag_id)
            session.add(DagWarning(dag_id, WARNING_TYPE, "Previous observation"))

    AssetPartitionWaitMonitor().check()

    warnings = _get_warnings()
    assert set(warnings) == {("first", WARNING_TYPE), ("second", WARNING_TYPE)}
    first_message = warnings["first", WARNING_TYPE][0]
    second_message = warnings["second", WARNING_TYPE][0]
    assert re.search(r"\b1\b", first_message)
    assert "3600" in first_message
    assert re.search(r"\b3\b", second_message)
    assert "1800" in second_message


@time_machine.travel(NOW, tick=False)
@pytest.mark.parametrize("fail_first_batch", [False, True])
def test_rotates_over_configured_dags_in_bounded_batches(fail_first_batch):
    dag_ids = [f"monitored-{index:03d}" for index in range(105)]
    with create_session(scoped=False) as session:
        for dag_id in dag_ids:
            _add_dag(session, dag_id)
            _add_pending(session, dag_id)
    monitor = AssetPartitionWaitMonitor()

    with conf_vars({TIMEOUT_OPTION: json.dumps(dict.fromkeys(dag_ids, 3600))}):
        if fail_first_batch:

            def fail_write(_connection, _cursor, statement, _parameters, _context, _executemany):
                if statement.lstrip().upper().startswith("INSERT INTO DAG_WARNING"):
                    raise RuntimeError("warning write failed")

            event.listen(settings.engine, "before_cursor_execute", fail_write)
            try:
                with pytest.raises(RuntimeError, match="warning write failed"):
                    monitor.check()
            finally:
                event.remove(settings.engine, "before_cursor_execute", fail_write)
            assert _get_warnings() == {}

        monitor.check()
        assert {dag_id for dag_id, _ in _get_warnings()} == set(dag_ids[:100])
        monitor.check()
        assert {dag_id for dag_id, _ in _get_warnings()} == set(dag_ids)

        with create_session(scoped=False) as session:
            session.execute(
                delete(AssetPartitionDagRun).where(AssetPartitionDagRun.target_dag_id == dag_ids[0])
            )
        monitor.check()

    assert {dag_id for dag_id, _ in _get_warnings()} == set(dag_ids[1:])


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: "{}"})
def test_disabled_policy_clears_existing_warnings_in_bounded_batches():
    dag_ids = [f"removed-{index:03d}" for index in range(105)]
    with create_session(scoped=False) as session:
        for dag_id in dag_ids:
            _add_dag(session, dag_id)
            session.add(DagWarning(dag_id, WARNING_TYPE, "Previous observation"))
    monitor = AssetPartitionWaitMonitor()

    monitor.check()
    assert {dag_id for dag_id, _ in _get_warnings()} == set(dag_ids[100:])
    monitor.check()

    assert _get_warnings() == {}


@time_machine.travel(NOW, tick=False)
def test_pending_stats_queries_do_not_scale_with_number_of_configured_dags():
    with create_session(scoped=False) as session:
        for index in range(50):
            dag_id = f"monitored-{index:03d}"
            _add_dag(session, dag_id)
            _add_pending(session, dag_id, age=timedelta(minutes=1))
    query_counts = []

    for count in (1, 50):
        policy = {f"monitored-{index:03d}": 3600 for index in range(count)}
        with conf_vars({TIMEOUT_OPTION: json.dumps(policy)}), count_queries() as queries:
            AssetPartitionWaitMonitor().check()
        query_counts.append(sum(queries.values()))

    assert query_counts == [3, 3]
    assert _get_warnings() == {}


@time_machine.travel(NOW, tick=False)
@pytest.mark.parametrize(
    "policy",
    [
        pytest.param("[]", id="list"),
        pytest.param("null", id="null"),
        pytest.param('{"monitored": true}', id="boolean"),
        pytest.param('{"monitored": "3600"}', id="numeric-string"),
        pytest.param('{"monitored": 0}', id="zero"),
        pytest.param('{"monitored": -1}', id="negative"),
        pytest.param('{"monitored": NaN}', id="nan"),
        pytest.param('{"monitored": Infinity}', id="infinity"),
        pytest.param('{"": 3600}', id="empty-dag-id"),
    ],
)
def test_invalid_policy_preserves_existing_warning(policy):
    with create_session(scoped=False) as session:
        _add_dag(session)
        session.add(DagWarning("monitored", WARNING_TYPE, "Previous observation", timestamp=NOW))
    before = _get_warnings()

    with (
        conf_vars({TIMEOUT_OPTION: policy}),
        pytest.raises(ValueError, match="pending_partitioned_dag_run_timeout"),
    ):
        AssetPartitionWaitMonitor().check()

    assert _get_warnings() == before


@time_machine.travel(NOW, tick=False)
@conf_vars({TIMEOUT_OPTION: '{"monitored": 3600}'})
def test_concurrent_monitors_create_one_warning():
    if settings.engine.dialect.name == "sqlite":
        pytest.skip("SQLite does not support concurrent read-then-write transactions")
    with create_session(scoped=False) as session:
        _add_dag(session)
        _add_pending(session)
    barrier = Barrier(2)

    def synchronize_warning_insert(_connection, _cursor, statement, _parameters, _context, _executemany):
        if statement.lstrip().upper().startswith("INSERT INTO DAG_WARNING"):
            barrier.wait(timeout=10)

    event.listen(settings.engine, "before_cursor_execute", synchronize_warning_insert)
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(AssetPartitionWaitMonitor().check) for _ in range(2)]
            for future in futures:
                future.result(timeout=20)
    finally:
        event.remove(settings.engine, "before_cursor_execute", synchronize_warning_insert)

    assert set(_get_warnings()) == {("monitored", WARNING_TYPE)}
