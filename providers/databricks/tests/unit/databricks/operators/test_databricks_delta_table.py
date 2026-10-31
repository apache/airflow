#
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

import re
from collections import defaultdict
from collections.abc import Sequence
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import DAG, AirflowSkipException
from airflow.providers.databricks.assets.databricks import UnityTableIdentity
from airflow.providers.databricks.exceptions import DatabricksDeltaTableError
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlExecutionError
from airflow.providers.databricks.operators.databricks_delta_table import (
    CHECKPOINT_KEY_EXTRA,
    DatabricksDeltaTableVersionProducerOperator,
    DeltaCheckpoint,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

HOST = "xx.cloud.databricks.com"
TABLE = UnityTableIdentity(host=HOST, catalog="Main", schema="Sales", table="Orders")
URI = f"databricks://{HOST}/main/sales/orders"
QUOTED = "`main`.`sales`.`orders`"
TABLE_ID = "11111111-1111-1111-1111-111111111111"
OTHER_TABLE_ID = "22222222-2222-2222-2222-222222222222"
KEY = "producer.watch_orders"
HOOK_PATH = "airflow.providers.databricks.operators.databricks_delta_table.DatabricksSqlHook"


class FakeDeltaTable:
    """In-memory stand-in for ``DESCRIBE DETAIL`` / ``DESCRIBE HISTORY`` on one table."""

    def __init__(
        self,
        commits: list[tuple[int, str]],
        *,
        table_id: str = TABLE_ID,
        table_format: str = "delta",
        retained_from: int = 0,
    ):
        self.commits = commits
        self.table_id = table_id
        self.table_format = table_format
        self.retained_from = retained_from
        self.statements: list[str] = []

    def run(self, sql: str, handler=None):
        self.statements.append(sql)
        if sql.startswith("DESCRIBE DETAIL"):
            return [SimpleNamespace(_asdict=lambda: {"format": self.table_format, "id": self.table_id})]
        match = re.fullmatch(r"DESCRIBE HISTORY (.+) LIMIT (\d+)", sql)
        assert match, sql
        limit = int(match.group(2))
        retained = [c for c in self.commits if c[0] >= self.retained_from]
        newest_first = sorted(retained, reverse=True)[:limit]
        return [
            SimpleNamespace(
                _asdict=lambda v=v, op=op: {
                    "version": v,
                    "operation": op,
                    "timestamp": datetime(2026, 10, 1, 12, v % 60, tzinfo=UTC),
                }
            )
            for v, op in newest_first
        ]


def make_event(extra: dict[str, Any]) -> SimpleNamespace:
    return SimpleNamespace(extra=extra)


def checkpoint_event(version: int, *, key: str = KEY, table_id: str = TABLE_ID) -> SimpleNamespace:
    return make_event({"version": version, "delta_table_id": table_id, CHECKPOINT_KEY_EXTRA: key})


class FilteringAccessor(Sequence):
    """Accessor shaped like the Airflow 3 one that filters events on the API server."""

    def __init__(self, events):
        self._events = list(events)
        self.calls: list[tuple] = []

    def extra(self, key, value):
        self.calls.append(("extra", key, value))
        self._events = [e for e in self._events if (e.extra or {}).get(key) == value]
        return self

    def ascending(self, ascending=True):
        self.calls.append(("ascending", ascending))
        if not ascending:
            self._events = list(reversed(self._events))
        return self

    def limit(self, limit):
        self.calls.append(("limit", limit))
        self._events = self._events[:limit]
        return self

    def __getitem__(self, i):
        return self._events[i]

    def __len__(self):
        return len(self._events)


def make_context(events=None, accessor=None) -> Any:
    """Return a minimal stand-in for the task ``Context`` with only the asset event accessors."""
    outlet_events: defaultdict = defaultdict(lambda: SimpleNamespace(extra={}))
    inlet = accessor if accessor is not None else list(events or [])
    return {"inlet_events": {TABLE.to_asset(): inlet}, "outlet_events": outlet_events}


def published(context) -> dict[str, Any] | None:
    for asset, event in context["outlet_events"].items():
        if asset.uri == URI:
            return event.extra
    return None


@pytest.fixture
def table():
    return FakeDeltaTable([(0, "CREATE TABLE"), (1, "WRITE"), (2, "WRITE")])


@pytest.fixture
def hook(table):
    with mock.patch(HOOK_PATH) as hook_cls:
        instance = hook_cls.return_value
        instance.host = HOST
        instance.run.side_effect = table.run
        yield instance


def make_operator(**kwargs) -> DatabricksDeltaTableVersionProducerOperator:
    kwargs.setdefault("checkpoint_key", KEY)
    return DatabricksDeltaTableVersionProducerOperator(
        task_id="watch_orders", unity_table=TABLE, http_path="/sql/1.0/warehouses/abc", **kwargs
    )


class TestConstruction:
    def test_defaults(self):
        op = make_operator(checkpoint_key=None)
        assert [a.uri for a in op.outlets] == [URI]
        assert [a.uri for a in op.inlets] == [URI]
        assert op.max_active_tis_per_dag == 1
        assert op.ignored_operations == ("OPTIMIZE", "VACUUM START", "VACUUM END")
        assert op.table_name == QUOTED

    def test_keeps_user_inlets_and_outlets(self):
        from airflow.providers.common.compat.assets import Asset

        other = Asset(uri="s3://bucket/other")
        op = make_operator(inlets=[other], outlets=[], max_active_tis_per_dag=4)
        assert [a.uri for a in op.inlets] == ["s3://bucket/other", URI]
        assert op.outlets == []
        assert op.max_active_tis_per_dag == 4

    def test_does_not_duplicate_inlet(self):
        op = make_operator(inlets=[TABLE.to_asset()])
        assert [a.uri for a in op.inlets] == [URI]

    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"baseline_version": -1}, "baseline_version"),
            ({"baseline_version": "3"}, "baseline_version"),
            ({"baseline_version": True}, "baseline_version"),
            ({"max_history_rows": 0}, "max_history_rows"),
            ({"on_recreation": "ignore"}, "on_recreation"),
            ({"checkpoint_key": ""}, "checkpoint_key"),
        ],
    )
    def test_rejects_invalid_arguments(self, kwargs, message):
        with pytest.raises(ValueError, match=message):
            make_operator(**kwargs)

    def test_quotes_backticks_in_identifiers(self):
        odd = UnityTableIdentity(host=HOST, catalog="main", schema="sales", table="we`ird")
        op = DatabricksDeltaTableVersionProducerOperator(task_id="t", unity_table=odd)
        assert op.table_name == "`main`.`sales`.`we``ird`"


class TestEnrollment:
    def test_first_run_publishes_enrollment_event(self, hook, table):
        context = make_context()
        result = make_operator().execute(context)

        assert published(context) == result
        assert result == {
            "version": 2,
            "observed_version": 2,
            "previous_version": None,
            "table_identity": {"host": HOST, "catalog": "main", "schema": "sales", "table": "orders"},
            "operation": "WRITE",
            "timestamp": "2026-10-01T12:02:00+00:00",
            "change": "enrollment",
            "delta_table_id": TABLE_ID,
            "dedup_id": f"{URI}@{TABLE_ID}:2",
            CHECKPOINT_KEY_EXTRA: KEY,
        }
        assert table.statements == [f"DESCRIBE DETAIL {QUOTED}", f"DESCRIBE HISTORY {QUOTED} LIMIT 1"]

    def test_baseline_at_current_version_skips(self, hook):
        context = make_context()
        with pytest.raises(AirflowSkipException, match="still at version 2"):
            make_operator(baseline_version=2).execute(context)
        assert published(context) is None

    def test_baseline_older_than_current_publishes_new_version(self, hook):
        context = make_context()
        result = make_operator(baseline_version=1).execute(context)
        assert result["change"] == "new_version"
        assert result["previous_version"] == 1
        assert result["version"] == 2

    def test_baseline_newer_than_table_fails(self, hook):
        with pytest.raises(DatabricksDeltaTableError, match="older than baseline_version 5"):
            make_operator(baseline_version=5).execute(make_context())

    def test_checkpoint_wins_over_baseline(self, hook):
        context = make_context([checkpoint_event(2)])
        with pytest.raises(AirflowSkipException):
            make_operator(baseline_version=0).execute(context)

    def test_default_checkpoint_key_is_dag_and_task(self, hook):
        with DAG("producer", schedule=None):
            op = make_operator(checkpoint_key=None)
        result = op.execute(make_context())
        assert result[CHECKPOINT_KEY_EXTRA] == "producer.watch_orders"


class TestVersionChanges:
    def test_unchanged_version_skips_without_event(self, hook):
        context = make_context([checkpoint_event(2)])
        with pytest.raises(AirflowSkipException, match="still at version 2"):
            make_operator().execute(context)
        assert published(context) is None

    def test_new_versions_are_coalesced_into_one_event_for_the_latest(self, hook, table):
        table.commits += [(3, "WRITE"), (4, "MERGE"), (5, "UPDATE")]
        context = make_context([checkpoint_event(2)])
        result = make_operator().execute(context)
        assert (result["change"], result["previous_version"], result["version"]) == ("new_version", 2, 5)
        assert result["operation"] == "UPDATE"
        assert published(context) == result
        # The head commit is a data change, so the operator does not scan further history.
        assert table.statements[-1] == f"DESCRIBE HISTORY {QUOTED} LIMIT 1"

    def test_only_maintenance_commits_skip(self, hook, table):
        table.commits += [(3, "OPTIMIZE"), (4, "VACUUM START"), (5, "VACUUM END")]
        context = make_context([checkpoint_event(2)])
        with pytest.raises(AirflowSkipException, match="only has"):
            make_operator().execute(context)
        assert published(context) is None
        assert table.statements[-1] == f"DESCRIBE HISTORY {QUOTED} LIMIT 3"

    def test_data_commit_hidden_behind_maintenance_publishes(self, hook, table):
        table.commits += [(3, "WRITE"), (4, "OPTIMIZE"), (5, "VACUUM END")]
        result = make_operator().execute(make_context([checkpoint_event(2)]))
        assert result["version"] == 5
        assert result["operation"] == "VACUUM END"

    def test_ignored_operations_are_case_insensitive(self, hook, table):
        table.commits += [(3, "optimize")]
        with pytest.raises(AirflowSkipException):
            make_operator().execute(make_context([checkpoint_event(2)]))

    def test_empty_ignored_operations_publish_for_every_commit(self, hook, table):
        table.commits += [(3, "OPTIMIZE")]
        result = make_operator(ignored_operations=()).execute(make_context([checkpoint_event(2)]))
        assert result["version"] == 3
        assert table.statements[-1] == f"DESCRIBE HISTORY {QUOTED} LIMIT 1"

    def test_expired_history_publishes_conservatively(self, hook, table):
        table.commits += [(3, "WRITE"), (4, "OPTIMIZE"), (5, "OPTIMIZE")]
        table.retained_from = 4  # Versions 3 and earlier were removed from the Delta log.
        result = make_operator().execute(make_context([checkpoint_event(2)]))
        assert result["version"] == 5

    def test_history_beyond_scan_window_publishes_conservatively(self, hook, table):
        table.commits += [(v, "OPTIMIZE") for v in range(3, 13)]
        result = make_operator(max_history_rows=5).execute(make_context([checkpoint_event(2)]))
        assert result["version"] == 12
        # It did not run the large scan at all.
        assert all("LIMIT 10" not in s for s in table.statements)


class TestRecreation:
    def test_new_table_id_publishes_recreated(self, hook, table):
        table.table_id = OTHER_TABLE_ID
        context = make_context([checkpoint_event(2)])
        result = make_operator().execute(context)
        assert result["change"] == "recreated"
        assert result["delta_table_id"] == OTHER_TABLE_ID
        assert result["dedup_id"] == f"{URI}@{OTHER_TABLE_ID}:2"

    def test_version_moved_backwards_publishes_recreated(self, hook, table):
        table.commits = [(0, "CREATE OR REPLACE TABLE")]
        result = make_operator().execute(make_context([checkpoint_event(7, table_id=None)]))
        assert (result["change"], result["previous_version"], result["version"]) == ("recreated", 7, 0)

    def test_recreated_with_only_maintenance_still_publishes(self, hook, table):
        table.table_id = OTHER_TABLE_ID
        table.commits = [(0, "CREATE TABLE"), (1, "OPTIMIZE")]
        result = make_operator().execute(make_context([checkpoint_event(1)]))
        assert result["change"] == "recreated"

    def test_on_recreation_fail_raises_and_publishes_nothing(self, hook, table):
        table.table_id = OTHER_TABLE_ID
        context = make_context([checkpoint_event(2)])
        with pytest.raises(DatabricksDeltaTableError, match="dropped and recreated"):
            make_operator(on_recreation="fail").execute(context)
        assert published(context) is None


class TestTableErrors:
    def test_non_delta_table_fails(self, hook, table):
        table.table_format = "parquet"
        context = make_context()
        with pytest.raises(DatabricksDeltaTableError, match="only Delta tables have versions"):
            make_operator().execute(context)
        assert published(context) is None

    def test_missing_or_inaccessible_table_propagates_and_publishes_nothing(self, hook):
        hook.run.side_effect = DatabricksSqlExecutionError("[TABLE_OR_VIEW_NOT_FOUND]")
        context = make_context([checkpoint_event(2)])
        with pytest.raises(DatabricksSqlExecutionError):
            make_operator().execute(context)
        assert published(context) is None

    def test_empty_history_fails(self, hook, table):
        table.commits = []
        with pytest.raises(DatabricksDeltaTableError, match="DESCRIBE HISTORY returned no rows"):
            make_operator().execute(make_context())

    def test_host_mismatch_fails_before_any_query(self, hook):
        hook.host = "other.cloud.databricks.com"
        with pytest.raises(ValueError, match="does not match unity_table host"):
            make_operator().execute(make_context())
        hook.run.assert_not_called()

    def test_host_match_is_case_insensitive(self, hook):
        hook.host = HOST.upper()
        assert make_operator().execute(make_context())["version"] == 2


class TestCheckpoint:
    def test_ignores_events_from_other_producers_and_scopes(self, hook):
        events = [
            checkpoint_event(1),
            checkpoint_event(2, key="another.producer"),
            make_event({"version": 2, "observed_version": 2}),  # Sensor outlet event.
            make_event({}),  # COPY INTO outlet event.
            make_event(None),
        ]
        op = make_operator()
        assert op.read_checkpoint(make_context(events)) == DeltaCheckpoint(version=1, table_id=TABLE_ID)

    def test_uses_newest_matching_event(self, hook):
        events = [checkpoint_event(1), checkpoint_event(2), make_event({})]
        assert make_operator().read_checkpoint(make_context(events)).version == 2

    def test_no_matching_event_means_no_checkpoint(self, hook):
        assert make_operator().read_checkpoint(make_context([make_event({})])) is None

    def test_uses_server_side_filter_when_available(self, hook):
        accessor = FilteringAccessor([checkpoint_event(1), checkpoint_event(5, key="x"), make_event({})])
        assert make_operator().read_checkpoint(make_context(accessor=accessor)).version == 1
        assert accessor.calls == [("extra", CHECKPOINT_KEY_EXTRA, KEY), ("ascending", False), ("limit", 1)]

    def test_corrupt_checkpoint_fails(self, hook):
        bad = make_event({CHECKPOINT_KEY_EXTRA: KEY, "version": "2"})
        with pytest.raises(DatabricksDeltaTableError, match="no integer version"):
            make_operator().read_checkpoint(make_context([bad]))


class TestDelivery:
    """
    The checkpoint is the published event, so these tests replay the producer across runs.

    A run that is killed before the task succeeds leaves no event; Airflow records the outlet
    event only on success. These tests model that by appending the event to the asset's
    history only for runs that complete.
    """

    def run_once(self, history: list, *, succeed: bool = True) -> dict[str, Any] | None:
        context = make_context(list(history))
        try:
            make_operator().execute(context)
        except AirflowSkipException:
            return None
        extra = published(context)
        if succeed:
            assert extra is not None
            history.append(make_event(extra))
        return extra

    def test_crash_before_publication_redelivers_same_version(self, hook, table):
        history: list = [checkpoint_event(2)]
        table.commits.append((3, "WRITE"))

        lost = self.run_once(history, succeed=False)
        retried = self.run_once(history)

        assert lost is not None
        assert retried == lost
        assert retried["dedup_id"] == f"{URI}@{TABLE_ID}:3"

    def test_crash_before_publication_coalesces_commits_made_meanwhile(self, hook, table):
        history: list = [checkpoint_event(2)]
        table.commits.append((3, "WRITE"))
        self.run_once(history, succeed=False)
        table.commits.append((4, "WRITE"))

        retried = self.run_once(history)
        assert (retried["previous_version"], retried["version"]) == (2, 4)

    def test_after_publication_checkpoint_has_advanced(self, hook, table):
        history: list = [checkpoint_event(2)]
        table.commits.append((3, "WRITE"))
        assert self.run_once(history)["version"] == 3
        assert self.run_once(history) is None
        table.commits.append((4, "WRITE"))
        assert self.run_once(history)["previous_version"] == 3

    def test_concurrent_runs_publish_same_dedup_id(self, hook, table):
        history: list = [checkpoint_event(2)]
        table.commits.append((3, "WRITE"))
        # Both runs read the checkpoint before either publishes.
        first_ctx, second_ctx = make_context(list(history)), make_context(list(history))
        first = make_operator().execute(first_ctx)
        second = make_operator().execute(second_ctx)
        assert first["dedup_id"] == second["dedup_id"]
        history += [make_event(first), make_event(second)]
        assert self.run_once(history) is None


@pytest.mark.db_test
@pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Runs the Dag through the Airflow 3 execution API")
class TestProducerDagRuns:
    """Run a producer Dag against the metadata database, so checkpoints are real asset events."""

    @pytest.fixture(autouse=True)
    def _clean_db(self):
        from tests_common.test_utils.db import clear_db_assets, clear_db_dags, clear_db_runs

        clear_db_runs()
        clear_db_assets()
        clear_db_dags()
        yield
        clear_db_runs()
        clear_db_assets()
        clear_db_dags()

    def test_events_and_skips_across_runs(self, hook, table):
        from sqlalchemy import select

        from airflow.models.asset import AssetEvent
        from airflow.utils.session import create_session

        from tests_common.test_utils.dag import sync_dag_to_db

        with DAG("delta_producer", schedule=None) as dag:
            DatabricksDeltaTableVersionProducerOperator(task_id="watch_orders", unity_table=TABLE)
        sync_dag_to_db(dag)

        states = []
        for commit in (None, None, (3, "WRITE"), (4, "OPTIMIZE"), None):
            if commit:
                table.commits.append(commit)
            states.append(dag.test().get_task_instance("watch_orders").state)

        with create_session() as session:
            events = session.scalars(select(AssetEvent).order_by(AssetEvent.id)).all()
            published = [(e.asset.uri, e.extra["change"], e.extra["version"]) for e in events]

        assert states == ["success", "skipped", "success", "skipped", "skipped"]
        assert published == [(URI, "enrollment", 2), (URI, "new_version", 3)]
