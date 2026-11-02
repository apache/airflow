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

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select

from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import InvalidPartitionKeyError
from airflow.models import DagModel, DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.simple import PartitionedAtRuntime
from airflow.timetables.trigger import CronPartitionTimetable
from airflow.utils.types import MAX_DAGRUN_TIMEOUT, DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    yield
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()


def test_trigger_dag_raises_error_if_manual_runs_denied(dag_maker, session):
    with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *"):
        EmptyOperator(task_id="mytask")
    session.commit()
    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == "TEST_DAG_1"))
    dag_model.allowed_run_types = ["scheduled"]
    session.commit()

    with pytest.raises(ValueError, match="Dag with dag_id: 'TEST_DAG_1' does not allow manual runs"):
        trigger_dag(
            dag_id="TEST_DAG_1",
            triggered_by=DagRunTriggeredByType.REST_API,
            session=session,
        )


def test_trigger_dag_with_custom_run_type(dag_maker, session):
    """Test that trigger_dag accepts and uses custom run_type parameter."""
    with dag_maker(session=session, dag_id="TEST_DAG_2", schedule=None):
        EmptyOperator(task_id="mytask")
    session.commit()

    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == "TEST_DAG_2"))
    dag_model.allowed_run_types = ["manual", "asset_materialization"]
    session.commit()

    dag_run = trigger_dag(
        dag_id="TEST_DAG_2",
        triggered_by=DagRunTriggeredByType.CLI,
        run_type=DagRunType.ASSET_MATERIALIZATION,
        session=session,
    )

    assert dag_run.run_type == DagRunType.ASSET_MATERIALIZATION


@pytest.mark.parametrize(
    "dagrun_timeout",
    [
        None,
        timedelta(minutes=30),
        timedelta(microseconds=250_000),
        timedelta(seconds=90, microseconds=123456),
        MAX_DAGRUN_TIMEOUT,
    ],
)
def test_trigger_dag_persists_timeout_override(dag_maker, session, dagrun_timeout):
    with dag_maker(session=session, dagrun_timeout=timedelta(hours=1)):
        EmptyOperator(task_id="mytask")
    session.commit()

    dag_run = trigger_dag(
        dag_id=dag_maker.dag.dag_id,
        triggered_by=DagRunTriggeredByType.REST_API,
        dagrun_timeout=dagrun_timeout,
        session=session,
    )
    run_id = dag_run.id
    session.flush()
    session.expire_all()

    stored_run = session.get(DagRun, run_id)
    expected_seconds = dagrun_timeout.total_seconds() if dagrun_timeout is not None else None
    assert stored_run.dagrun_timeout_seconds == expected_seconds
    assert dag_maker.serialized_dag.dagrun_timeout == timedelta(hours=1)


@pytest.mark.parametrize(
    ("dagrun_timeout", "exception", "message"),
    [
        (timedelta(0), ValueError, "dagrun_timeout must be positive"),
        (timedelta(seconds=-1), ValueError, "dagrun_timeout must be positive"),
        (30, TypeError, "dagrun_timeout must be a datetime.timedelta"),
        (
            MAX_DAGRUN_TIMEOUT + timedelta(microseconds=1),
            ValueError,
            "dagrun_timeout must not exceed 999999999 days",
        ),
    ],
)
def test_trigger_dag_rejects_invalid_timeout(dag_maker, session, dagrun_timeout, exception, message):
    with dag_maker(session=session):
        EmptyOperator(task_id="mytask")
    session.commit()

    with pytest.raises(exception, match=message):
        trigger_dag(
            dag_id=dag_maker.dag.dag_id,
            triggered_by=DagRunTriggeredByType.REST_API,
            dagrun_timeout=dagrun_timeout,
            session=session,
        )

    assert session.scalar(select(DagRun).where(DagRun.dag_id == dag_maker.dag.dag_id)) is None


def test_trigger_dag_populates_partition_date_for_cron_partition_timetable(dag_maker, session):
    """Manually triggering a CronPartitionTimetable Dag with a partition_key populates partition_date."""
    with dag_maker(
        session=session,
        dag_id="TEST_CRON_PARTITION_DAG",
        schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
    ):
        EmptyOperator(task_id="mytask")
    session.commit()

    dag_run = trigger_dag(
        dag_id="TEST_CRON_PARTITION_DAG",
        triggered_by=DagRunTriggeredByType.REST_API,
        partition_key="2025-06-01T00:00:00",
        session=session,
    )

    assert dag_run is not None
    assert dag_run.partition_key == "2025-06-01T00:00:00"
    assert dag_run.partition_date == datetime(2025, 6, 1, 0, 0, 0, tzinfo=UTC)


def test_trigger_dag_raises_invalid_partition_key_for_cron_partition_timetable(dag_maker, session):
    """A malformed partition_key for CronPartitionTimetable raises InvalidPartitionKeyError."""
    with dag_maker(
        session=session,
        dag_id="TEST_CRON_PARTITION_BAD_KEY",
        schedule=CronPartitionTimetable("0 0 * * *", timezone="UTC"),
    ):
        EmptyOperator(task_id="mytask")
    session.commit()

    with pytest.raises(InvalidPartitionKeyError, match="does not match"):
        trigger_dag(
            dag_id="TEST_CRON_PARTITION_BAD_KEY",
            triggered_by=DagRunTriggeredByType.REST_API,
            partition_key="not-a-valid-date",
            session=session,
        )


def test_trigger_dag_partitioned_at_runtime_leaves_partition_date_none(dag_maker, session):
    """PartitionedAtRuntime Dags accept arbitrary keys; partition_date stays None."""
    with dag_maker(
        session=session,
        dag_id="TEST_PARTITIONED_AT_RUNTIME",
        schedule=PartitionedAtRuntime(),
    ):
        EmptyOperator(task_id="mytask")
    session.commit()

    dag_run = trigger_dag(
        dag_id="TEST_PARTITIONED_AT_RUNTIME",
        triggered_by=DagRunTriggeredByType.REST_API,
        partition_key="arbitrary-runtime-key",
        session=session,
    )

    assert dag_run is not None
    assert dag_run.partition_key == "arbitrary-runtime-key"
    assert dag_run.partition_date is None


def test_trigger_dag_operator_denied_when_only_manual_allowed(dag_maker, session):
    with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *"):
        EmptyOperator(task_id="mytask")
    session.commit()
    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == "TEST_DAG_1"))
    dag_model.allowed_run_types = ["manual"]
    session.commit()

    with pytest.raises(
        ValueError, match="Dag with dag_id: 'TEST_DAG_1' does not allow operator_triggered runs"
    ):
        trigger_dag(
            dag_id="TEST_DAG_1",
            run_type=DagRunType.OPERATOR_TRIGGERED,
            triggered_by=DagRunTriggeredByType.OPERATOR,
            session=session,
        )
