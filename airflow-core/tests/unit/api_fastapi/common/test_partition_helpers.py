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

from unittest import mock

import pendulum
import pytest
from sqlalchemy.orm import Session

from airflow.api_fastapi.common.partition_helpers import (
    _extract_partitioned_timetable,
    suggest_partition_key_for_dag,
)
from airflow.exceptions import DeserializationError
from airflow.models.asset import AssetPartitionDagRun
from airflow.partition_mappers.temporal import StartOfDayMapper
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from airflow.serialization.encoders import ensure_serialized_asset
from airflow.timetables.simple import NullTimetable, PartitionedAssetTimetable, PartitionedAtRuntime
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

NOW = pendulum.datetime(2025, 6, 1, tz="UTC")


def _make_serdag(exc: Exception):
    """
    Return a plain SerializedDagModel stand-in whose ``.dag`` property raises ``exc``.

    Plain class (not ``MagicMock``) because ``MagicMock.__getattr__`` swallows
    ``AttributeError`` from descriptors and falls back to auto-attribute creation,
    which masks the ``except AttributeError`` branch we want to exercise.
    """

    class _SerDag:
        dag_id = "bad-dag"

        @property
        def dag(self):
            raise exc

    return _SerDag()


@pytest.mark.parametrize(
    "exc",
    [
        DeserializationError("corrupted"),
        TypeError("mis-paired RollupMapper upstream/window"),
    ],
    ids=["DeserializationError", "TypeError"],
)
def test_extract_partitioned_timetable_deserialization_failure_logs_and_returns_none(exc):
    """
    Failures from the timetable deserialization path (``DeserializationError``)
    and from ``RollupMapper.__init__``'s eager pairing validation (``TypeError``)
    must produce a warning log and return ``None`` rather than propagating —
    protects the read-only UI from 500s when a serialized Dag is corrupted or
    misconfigured.
    """
    serdag = _make_serdag(exc)

    with mock.patch("airflow.api_fastapi.common.partition_helpers.log") as mock_log:
        result = _extract_partitioned_timetable(serdag)

    assert result is None
    assert mock_log.warning.mock_calls == [
        mock.call("Failed to deserialize timetable for Dag", dag_id="bad-dag", exc_info=True)
    ]


@pytest.mark.parametrize(
    "exc",
    [
        KeyError("missing key"),
        AttributeError("no attr"),
        ImportError("no module"),
        ValueError("bad value"),
        RuntimeError("unexpected"),
    ],
    ids=["KeyError", "AttributeError", "ImportError", "ValueError", "RuntimeError"],
)
def test_extract_partitioned_timetable_refactor_signal_exceptions_propagate(exc):
    """
    Exceptions outside the narrow ``(DeserializationError, TypeError)`` set must
    propagate so refactor bugs (renamed attribute, missing dict key, broken
    import path) and runtime errors surface to the caller instead of silently
    downgrading the route to non-rollup.
    """
    serdag = _make_serdag(exc)

    with mock.patch("airflow.api_fastapi.common.partition_helpers.log") as mock_log:
        with pytest.raises(type(exc)):
            _extract_partitioned_timetable(serdag)

    mock_log.warning.assert_not_called()


def _build_asset_driven_timetable(asset_uri: str) -> PartitionedAssetTimetable:
    """
    Asset-driven timetable using the real default mapper (``IdentityMapper``).

    ``IdentityMapper`` has no ``format_period_containing``, so
    ``suggest_partition_key`` always returns ``None`` for it — exercising the
    resolver's fallback to the recent-run source (step 3) without a temporal
    mapper (step 2) short-circuiting first.
    """
    return PartitionedAssetTimetable(assets=ensure_serialized_asset(Asset(name=asset_uri, uri=asset_uri)))


@pytest.mark.db_test
class TestSuggestPartitionKeyForDag:
    @pytest.mark.parametrize(
        "timetable_kind",
        ["partitioned_at_runtime", "asset_driven"],
    )
    def test_returns_partition_key_of_most_recent_successful_run(self, dag_maker, session, timetable_kind):
        dag_id = f"suggest_pk_recent_{timetable_kind}"
        timetable = (
            PartitionedAtRuntime()
            if timetable_kind == "partitioned_at_runtime"
            else _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(
            run_id="older",
            state=DagRunState.SUCCESS,
            partition_key="2024-01-01",
            logical_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        )
        dag_maker.create_dagrun(
            run_id="newer",
            state=DagRunState.SUCCESS,
            partition_key="2024-01-02",
            logical_date=pendulum.datetime(2024, 1, 2, tz="UTC"),
        )
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "2024-01-02"

    @pytest.mark.parametrize(
        "timetable_kind",
        ["partitioned_at_runtime", "asset_driven"],
    )
    def test_returns_none_when_dag_never_ran(self, dag_maker, session, timetable_kind):
        dag_id = f"suggest_pk_never_ran_{timetable_kind}"
        timetable = (
            PartitionedAtRuntime()
            if timetable_kind == "partitioned_at_runtime"
            else _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result is None

    @pytest.mark.parametrize(
        "timetable_kind",
        ["partitioned_at_runtime", "asset_driven"],
    )
    def test_returns_none_when_most_recent_successful_run_is_unpartitioned(
        self, dag_maker, session, timetable_kind
    ):
        dag_id = f"suggest_pk_unpartitioned_{timetable_kind}"
        timetable = (
            PartitionedAtRuntime()
            if timetable_kind == "partitioned_at_runtime"
            else _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(
            run_id="older",
            state=DagRunState.SUCCESS,
            partition_key="2024-01-01",
            logical_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        )
        dag_maker.create_dagrun(
            run_id="newer",
            state=DagRunState.SUCCESS,
            partition_key=None,
            logical_date=pendulum.datetime(2024, 1, 2, tz="UTC"),
        )
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result is None

    def test_pending_apdr_takes_priority_over_recent_run(self, dag_maker, session):
        dag_id = "suggest_pk_pending_apdr_dag"
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="older", state=DagRunState.SUCCESS, partition_key="2024-01-01")
        session.add(AssetPartitionDagRun(target_dag_id=dag_id, partition_key="pending-key"))
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "pending-key"

    def test_falls_back_to_mapper_derived_key_when_no_apdr_or_run(self, dag_maker, session):
        dag_id = "suggest_pk_mapper_derived_dag"
        timetable = PartitionedAssetTimetable(
            assets=Asset(name=f"s3://bucket/{dag_id}", uri=f"s3://bucket/{dag_id}"),
            default_partition_mapper=StartOfDayMapper(),
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "2025-06-01"

    def test_pending_apdr_takes_priority_over_mapper_derived_key(self, dag_maker, session):
        dag_id = "suggest_pk_pending_beats_mapper_dag"
        timetable = PartitionedAssetTimetable(
            assets=Asset(name=f"s3://bucket/{dag_id}", uri=f"s3://bucket/{dag_id}"),
            default_partition_mapper=StartOfDayMapper(),
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        session.add(AssetPartitionDagRun(target_dag_id=dag_id, partition_key="pending-key"))
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "pending-key"

    def test_mapper_derived_key_takes_priority_over_recent_run(self, dag_maker, session):
        dag_id = "suggest_pk_mapper_beats_run_dag"
        timetable = PartitionedAssetTimetable(
            assets=Asset(name=f"s3://bucket/{dag_id}", uri=f"s3://bucket/{dag_id}"),
            default_partition_mapper=StartOfDayMapper(),
        )
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="older", state=DagRunState.SUCCESS, partition_key="2024-01-01")
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "2025-06-01"

    @pytest.mark.parametrize(
        ("dag_id", "apdrs", "expected"),
        [
            pytest.param(
                "suggest_pk_apdr_fifo_created_at_dag",
                [("newest-pending", "2025-05-02"), ("oldest-pending", "2025-05-01")],
                "oldest-pending",
                id="created_at-order-opposite-to-id-order",
            ),
            pytest.param(
                "suggest_pk_apdr_fifo_same_created_at_dag",
                [("b-smaller-id", "2025-05-01"), ("a-larger-id", "2025-05-01")],
                "b-smaller-id",
                id="same-created_at-smaller-id-wins",
            ),
        ],
    )
    def test_pending_apdr_is_picked_in_scheduler_fifo_order(
        self, dag_maker, session, dag_id, apdrs, expected
    ):
        """The oldest pending APDR wins by ``(created_at, id)``, matching the scheduler's claim order."""
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        for partition_key, created_at in apdrs:
            session.add(
                AssetPartitionDagRun(
                    target_dag_id=dag_id,
                    partition_key=partition_key,
                    created_at=pendulum.parse(created_at, tz="UTC"),
                )
            )
            session.flush()
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == expected

    def test_apdr_that_already_created_a_run_is_skipped(self, dag_maker, session):
        dag_id = "suggest_pk_apdr_created_dag"
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        created_run = dag_maker.create_dagrun(run_id="created", state=DagRunState.RUNNING)
        session.add(
            AssetPartitionDagRun(
                target_dag_id=dag_id,
                partition_key="already-created",
                created_dag_run_id=created_run.id,
                created_at=pendulum.datetime(2025, 5, 1, tz="UTC"),
            )
        )
        session.add(
            AssetPartitionDagRun(
                target_dag_id=dag_id,
                partition_key="still-pending",
                created_at=pendulum.datetime(2025, 5, 2, tz="UTC"),
            )
        )
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "still-pending"

    def test_pending_apdr_of_another_dag_is_ignored(self, dag_maker, session):
        dag_id = "suggest_pk_apdr_own_dag"
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        session.add(AssetPartitionDagRun(target_dag_id="some_other_dag", partition_key="other-dag-key"))
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result is None

    def test_only_successful_runs_are_considered_even_when_newer_runs_exist(self, dag_maker, session):
        dag_id = "suggest_pk_only_success_dag"
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(
            run_id="succeeded",
            state=DagRunState.SUCCESS,
            partition_key="2024-01-01",
            logical_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        )
        dag_maker.create_dagrun(
            run_id="failed",
            state=DagRunState.FAILED,
            partition_key="2024-01-02",
            logical_date=pendulum.datetime(2024, 1, 2, tz="UTC"),
        )
        dag_maker.create_dagrun(
            run_id="running",
            state=DagRunState.RUNNING,
            partition_key="2024-01-03",
            logical_date=pendulum.datetime(2024, 1, 3, tz="UTC"),
        )
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "2024-01-01"

    @pytest.mark.parametrize("run_type", [DagRunType.BACKFILL_JOB, DagRunType.MANUAL])
    def test_most_recent_successful_run_is_picked_regardless_of_run_type(self, dag_maker, session, run_type):
        """Pins current behaviour: the run with the largest ``DagRun.id`` wins, whatever its ``run_type``."""
        dag_id = f"suggest_pk_run_type_{run_type.value}_dag"
        timetable = _build_asset_driven_timetable(f"s3://bucket/{dag_id}")
        with dag_maker(dag_id=dag_id, schedule=timetable, serialized=True, session=session):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(
            run_id="recent-asset-run",
            run_type=DagRunType.ASSET_TRIGGERED,
            state=DagRunState.SUCCESS,
            partition_key="2025-05-31",
            logical_date=None,
        )
        dag_maker.create_dagrun(
            run_id="later-created-old-partition",
            run_type=run_type,
            state=DagRunState.SUCCESS,
            partition_key="2024-01-01",
            logical_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        )
        session.commit()

        result = suggest_partition_key_for_dag(dag_id=dag_id, timetable=timetable, now=NOW, session=session)
        assert result == "2024-01-01"

    def test_returns_none_and_issues_no_query_for_non_partitioned_timetable(self):
        mock_session = mock.create_autospec(Session, instance=True)

        result = suggest_partition_key_for_dag(
            dag_id="not-partitioned-dag", timetable=NullTimetable(), now=NOW, session=mock_session
        )

        assert result is None
        assert mock_session.mock_calls == []
