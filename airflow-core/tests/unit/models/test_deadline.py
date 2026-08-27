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

from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select

from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
from airflow.models import DagRun
from airflow.models.deadline import Deadline, ReferenceModels
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.deadline import (
    DeadlineReference,
)
from airflow.serialization.definitions.deadline import (
    SerializedReferenceModels,
)
from airflow.utils.state import DagRunState

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "dag_id_1"
INVALID_DAG_ID = "invalid_dag_id"
INVALID_RUN_ID = -1


async def callback_for_deadline():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


TEST_CALLBACK_PATH = f"{__name__}.{callback_for_deadline.__name__}"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_ASYNC_CALLBACK = AsyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)
TEST_SYNC_CALLBACK = SyncCallback(TEST_CALLBACK_PATH, kwargs=TEST_CALLBACK_KWARGS)

ORIGINAL_DAGRUN_QUEUED = frozenset(DeadlineReference.TYPES.DAGRUN_QUEUED)
ORIGINAL_DAGRUN_CREATED = frozenset(DeadlineReference.TYPES.DAGRUN_CREATED)


def _clean_db():
    db.clear_db_dags()
    db.clear_db_runs()
    db.clear_db_deadline()
    db.clear_db_dag_bundles()
    db.clear_db_teams()


def assert_correct_timing(reference, expected_timing):
    assert reference in DeadlineReference.TYPES.DAGRUN
    if expected_timing == DeadlineReference.TYPES.DAGRUN_CREATED:
        assert reference in DeadlineReference.TYPES.DAGRUN_CREATED
        assert reference not in DeadlineReference.TYPES.DAGRUN_QUEUED
    elif expected_timing == DeadlineReference.TYPES.DAGRUN_QUEUED:
        assert reference in DeadlineReference.TYPES.DAGRUN_QUEUED
        assert reference not in DeadlineReference.TYPES.DAGRUN_CREATED


def assert_builtin_types_unchanged(current_queued, current_created):
    for builtin_type in ORIGINAL_DAGRUN_CREATED:
        assert builtin_type in current_created
    for builtin_type in ORIGINAL_DAGRUN_QUEUED:
        assert builtin_type in current_queued


@pytest.fixture
def dagrun(session, dag_maker):
    with dag_maker(DAG_ID):
        EmptyOperator(task_id="TASK_ID")
    with time_machine.travel(DEFAULT_DATE):
        dag_maker.create_dagrun(state=DagRunState.QUEUED, logical_date=DEFAULT_DATE)

        session.commit()
        dag_runs = session.scalars(select(DagRun)).all()
        assert len(dag_runs) == 1
        return dag_runs[0]


@pytest.fixture
def deadline_orm(dagrun, session):
    with time_machine.travel(DEFAULT_DATE, tick=False):
        deadline = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            deadline_alert_id=None,
        )
        session.add(deadline)
        session.flush()
        return deadline


@pytest.mark.db_test
class TestDeadline:
    @staticmethod
    def teardown_method():
        _clean_db()

    @pytest.mark.parametrize(
        "conditions",
        [
            pytest.param({}, id="empty_conditions"),
            pytest.param({Deadline.dagrun_id: -1}, id="no_matches"),
            pytest.param({Deadline.dagrun_id: "valid_placeholder"}, id="single_condition"),
            pytest.param(
                {
                    Deadline.dagrun_id: "valid_placeholder",
                    Deadline.deadline_time: datetime.now() + timedelta(days=365),
                },
                id="multiple_conditions",
            ),
            pytest.param(
                {Deadline.dagrun_id: "valid_placeholder", Deadline.callback: None},
                id="mixed_conditions",
            ),
        ],
    )
    @mock.patch("sqlalchemy.orm.Session")
    def test_prune_deadlines(self, mock_session, conditions, dagrun):
        """Test deadline resolution with various conditions."""
        if Deadline.dagrun_id in conditions:
            if conditions[Deadline.dagrun_id] == "valid_placeholder":
                conditions[Deadline.dagrun_id] = dagrun.id

        expected_result = 1 if conditions else 0
        # Set up the query chain to return a list of (Deadline, DagRun) pairs
        mock_dagrun = mock.Mock(spec=DagRun, end_date=datetime.now())
        mock_deadline = mock.Mock(spec=Deadline, deadline_time=mock_dagrun.end_date + timedelta(days=365))
        mock_query = mock_session.execute.return_value
        mock_query.all.return_value = [(mock_deadline, mock_dagrun)] if conditions else []

        result = Deadline.prune_deadlines(conditions=conditions, session=mock_session)
        assert result == expected_result
        if conditions:
            mock_session.execute.return_value.all.assert_called_once()
            mock_session.delete.assert_called_once_with(mock_deadline)
        else:
            mock_session.execute.assert_not_called()

    def test_repr_with_callback_kwargs(self, deadline_orm, dagrun):
        repr_str = repr(deadline_orm)
        assert "[DagRun Deadline]" in repr_str
        assert f"created at {DEFAULT_DATE}" in repr_str
        assert f"Dag: {DAG_ID}" in repr_str
        assert f"Run: {dagrun.id}" in repr_str
        assert f"needed by {DEFAULT_DATE}" in repr_str
        assert TEST_CALLBACK_PATH in repr_str
        assert str(TEST_CALLBACK_KWARGS) in repr_str

    def test_repr_without_callback_kwargs(self, dagrun, session):
        with time_machine.travel(DEFAULT_DATE, tick=False):
            # Create a new Deadline without callback kwargs.
            deadline = Deadline(
                deadline_time=DEFAULT_DATE,
                callback=AsyncCallback(TEST_CALLBACK_PATH),
                dagrun_id=dagrun.id,
                deadline_alert_id=None,
            )
            session.add(deadline)
            session.flush()

            assert not deadline.callback.data.get("kwargs")
            repr_str = repr(deadline)
            assert "[DagRun Deadline]" in repr_str
            assert f"created at {DEFAULT_DATE}" in repr_str
            assert f"Dag: {DAG_ID}" in repr_str
            assert f"Run: {dagrun.id}" in repr_str
            assert f"needed by {DEFAULT_DATE}" in repr_str
            assert TEST_CALLBACK_PATH in repr_str

    def test_repr_with_dagrun_id_but_no_dagrun_relationship(self, deadline_orm):
        """__repr__ must NOT raise when dagrun_id is set but the dagrun relationship is None.

        The FK (dagrun_id) can be set while the relationship resolves to None — e.g. the DagRun
        was deleted (ondelete=CASCADE) and this is a stale/expired in-memory Deadline. A __repr__
        that raised AttributeError here would break log lines, tracebacks, and debugger displays
        exactly when something is already going wrong. The repr falls back to an id-only form.
        """
        # Sever the relationship while keeping the FK id (simulates deleted/detached DagRun).
        deadline_orm.dagrun = None
        assert deadline_orm.dagrun_id is not None

        repr_str = repr(deadline_orm)  # must not raise
        assert "[DagRun Deadline]" in repr_str
        assert f"Run: {deadline_orm.dagrun_id}" in repr_str
        assert "Dag: <unknown>" in repr_str

    @pytest.mark.db_test
    def test_bundle_name_propagated_to_callback(self, dagrun, session):
        """The bundle name is forwarded to the callback so the triggerer can resolve its team."""
        deadline = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
            bundle_name="my_bundle",
        )
        session.add(deadline)
        session.flush()

        assert deadline.callback.bundle_name == "my_bundle"

    @pytest.mark.db_test
    def test_handle_miss(self, dagrun, session):
        deadline_orm = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
        )
        session.add(deadline_orm)
        session.flush()
        assert not deadline_orm.missed

        with mock.patch.object(deadline_orm.callback, "queue") as mock_queue:
            deadline_orm.handle_miss(session)
            session.flush()
            mock_queue.assert_called_once()

        assert deadline_orm.missed

        callback_kwargs = deadline_orm.callback.data["kwargs"]
        context = callback_kwargs.pop("context")
        assert callback_kwargs == TEST_CALLBACK_KWARGS

        assert context["deadline"]["id"] == str(deadline_orm.id)
        assert context["deadline"]["deadline_time"].timestamp() == deadline_orm.deadline_time.timestamp()
        assert context["dag_run"] == DAGRunResponse.model_validate(dagrun).model_dump(mode="json")

    @pytest.mark.db_test
    def test_handle_miss_persists_triggerer_callback_context(self, dagrun, session):
        deadline_orm = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=AsyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
        )
        session.add(deadline_orm)
        session.flush()

        callback_id = deadline_orm.callback.id
        deadline_id = deadline_orm.id
        deadline_time = deadline_orm.deadline_time
        expected_dag_run = DAGRunResponse.model_validate(dagrun).model_dump(mode="json")

        deadline_orm.handle_miss(session)
        session.commit()
        session.expunge_all()

        callback = session.scalar(select(Deadline).where(Deadline.id == deadline_id)).callback
        assert callback.id == callback_id

        callback_kwargs = callback.data["kwargs"]
        context = callback_kwargs["context"]
        assert {
            key: value for key, value in callback_kwargs.items() if key != "context"
        } == TEST_CALLBACK_KWARGS
        assert context["deadline"]["id"] == str(deadline_id)
        assert context["deadline"]["deadline_time"].timestamp() == deadline_time.timestamp()
        assert context["dag_run"] == expected_dag_run

        callback.trigger = None
        session.commit()

    @pytest.mark.db_test
    def test_handle_miss_persists_executor_callback_routing_data(self, dagrun, session):
        deadline_orm = Deadline(
            deadline_time=DEFAULT_DATE,
            callback=SyncCallback(TEST_CALLBACK_PATH, TEST_CALLBACK_KWARGS),
            dagrun_id=dagrun.id,
            dag_id=dagrun.dag_id,
            deadline_alert_id=None,
        )
        session.add(deadline_orm)
        session.flush()

        callback_id = deadline_orm.callback.id
        deadline_id = deadline_orm.id
        dagrun_id = dagrun.id
        dag_id = dagrun.dag_id

        deadline_orm.handle_miss(session)
        session.commit()
        session.expunge_all()

        callback = session.scalar(select(Deadline).where(Deadline.id == deadline_id)).callback
        assert callback.id == callback_id
        assert callback.data["dag_run_id"] == str(dagrun_id)
        assert callback.data["dag_id"] == dag_id
        assert callback.data["deadline_id"] == str(deadline_id)


@pytest.mark.db_test
class TestCalculatedDeadlineReferences:
    @staticmethod
    def teardown_method():
        _clean_db()

    @pytest.mark.parametrize(
        ("reference", "attribute"),
        [
            pytest.param(
                SerializedReferenceModels.DagRunLogicalDateDeadline(), "logical_date", id="logical_date"
            ),
            pytest.param(SerializedReferenceModels.DagRunQueuedAtDeadline(), "queued_at", id="queued_at"),
            pytest.param(
                ReferenceModels.DagRunLogicalDateDeadline(), "logical_date", id="legacy_logical_date"
            ),
            pytest.param(ReferenceModels.DagRunQueuedAtDeadline(), "queued_at", id="legacy_queued_at"),
        ],
    )
    def test_dagrun_references_use_supplied_dagrun(self, reference, attribute, session):
        """DagRun references use the in-memory DagRun instead of querying it again."""
        dagrun = SimpleNamespace(dag_id=DAG_ID, logical_date=DEFAULT_DATE, queued_at=DEFAULT_DATE)
        interval = timedelta(hours=1)

        assert getattr(dagrun, attribute) == DEFAULT_DATE
        assert (
            reference.evaluate_with(
                session=session,
                interval=interval,
                dagrun=dagrun,
                dag_id=DAG_ID,
                run_id="dagrun_1",
                unexpected="ignored",
            )
            == DEFAULT_DATE + interval
        )

    @pytest.mark.parametrize(
        ("reference", "attribute", "message"),
        [
            pytest.param(
                SerializedReferenceModels.DagRunLogicalDateDeadline(),
                "logical_date",
                "No deadline created for dag_id_1: the Dag run has no logical date.",
                id="logical_date",
            ),
            pytest.param(
                SerializedReferenceModels.DagRunQueuedAtDeadline(),
                "queued_at",
                "No deadline created for dag_id_1: the Dag run has no queued at time.",
                id="queued_at",
            ),
        ],
    )
    def test_dagrun_references_log_when_dagrun_date_is_missing(
        self, reference, attribute, message, caplog, session
    ):
        caplog.set_level("WARNING", logger=reference.log.name)
        dagrun = SimpleNamespace(dag_id=DAG_ID, logical_date=DEFAULT_DATE, queued_at=DEFAULT_DATE)
        setattr(dagrun, attribute, None)

        assert reference.evaluate_with(session=session, interval=timedelta(), dagrun=dagrun) is None
        assert caplog.messages == [message]
