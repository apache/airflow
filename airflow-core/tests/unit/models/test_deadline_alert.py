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

from datetime import timedelta
from unittest.mock import Mock

import pytest
import time_machine
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk.definitions.deadline import BaseDeadlineReference, DeadlineReference
from airflow.serialization.definitions.deadline import SerializedReferenceModels

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DAG_ID = "test_deadline_alert_dag"
DEADLINE_NAME = "Test Alert"
DEADLINE_DESCRIPTION = "This is a test alert description"
DEADLINE_INTERVAL = 60
DEADLINE_CALLBACK = {"path": "test.callback"}
SERIALIZED_DAG_ID = "serialized_dag_uuid"


def _clean_db():
    db.clear_db_deadline_alert()


@pytest.fixture
def deadline_reference():
    return DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference()


@pytest.fixture
def deadline_alert_orm(dag_maker, session, deadline_reference):
    with dag_maker(DAG_ID, session=session):
        pass

    serialized_dag = session.execute(
        select(SerializedDagModel).where(SerializedDagModel.dag_id == DAG_ID)
    ).scalar_one()

    with time_machine.travel(DEFAULT_DATE, tick=False):
        alert = DeadlineAlert(
            serialized_dag_id=serialized_dag.id,
            name=DEADLINE_NAME,
            description=DEADLINE_DESCRIPTION,
            reference=deadline_reference,
            interval=DEADLINE_INTERVAL,
            callback_def=DEADLINE_CALLBACK,
        )
        session.add(alert)
        session.flush()
        return alert


@pytest.mark.db_test
class TestDeadlineAlert:
    @staticmethod
    def setup_method():
        _clean_db()

    @staticmethod
    def teardown_method():
        _clean_db()

    def test_deadline_alert_creation(self, deadline_alert_orm):
        assert deadline_alert_orm.id is not None
        assert deadline_alert_orm.created_at == DEFAULT_DATE
        assert deadline_alert_orm.name == DEADLINE_NAME
        assert deadline_alert_orm.description == DEADLINE_DESCRIPTION

    def test_minimal_deadline_alert_creation(self, dag_maker, session, deadline_reference):
        with dag_maker(DAG_ID, session=session):
            pass

        serialized_dag = session.execute(
            select(SerializedDagModel).where(SerializedDagModel.dag_id == DAG_ID)
        ).scalar_one()

        with time_machine.travel(DEFAULT_DATE, tick=False):
            deadline_alert = DeadlineAlert(
                serialized_dag_id=serialized_dag.id,
                reference=deadline_reference,
                interval=DEADLINE_INTERVAL,
                callback_def=DEADLINE_CALLBACK,
            )
            session.add(deadline_alert)
            session.flush()

            assert deadline_alert.id is not None
            assert deadline_alert.created_at == DEFAULT_DATE
            assert deadline_alert.name is None
            assert deadline_alert.description is None

    def test_deadline_alert_repr(self, deadline_alert_orm, deadline_reference):
        repr_str = repr(deadline_alert_orm)
        assert "[DeadlineAlert]" in repr_str
        assert "id=" in repr_str
        assert f"created_at={DEFAULT_DATE}" in repr_str
        assert f"name={DEADLINE_NAME}" in repr_str
        assert f"reference={deadline_reference}" in repr_str
        assert "interval=1m" in repr_str
        assert repr(deadline_alert_orm.callback_def) in repr_str

    def test_deadline_alert_matches_definition(self, session, deadline_reference):
        alert1 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=deadline_reference,
            interval=DEADLINE_INTERVAL,
            callback_def=DEADLINE_CALLBACK,
        )
        alert2 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=deadline_reference,
            interval=DEADLINE_INTERVAL,
            callback_def=DEADLINE_CALLBACK,
        )
        assert alert1.matches_definition(alert2)

        different_ref = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DeadlineReference.DAGRUN_LOGICAL_DATE.serialize_reference(),
            interval=DEADLINE_INTERVAL,
            callback_def=DEADLINE_CALLBACK,
        )
        assert not alert1.matches_definition(different_ref)

        different_interval = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=deadline_reference,
            interval=120,
            callback_def=DEADLINE_CALLBACK,
        )
        assert not alert1.matches_definition(different_interval)

        different_callback = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=deadline_reference,
            interval=DEADLINE_INTERVAL,
            callback_def={"path": "different.callback"},
        )
        assert not alert1.matches_definition(different_callback)

        assert alert1.matches_definition("not a deadline alert") is NotImplemented

    def test_deadline_alert_reference_class_property(self, deadline_alert_orm):
        assert deadline_alert_orm.reference_class == SerializedReferenceModels.DagRunQueuedAtDeadline

    def test_deadline_alert_get_by_id(self, deadline_alert_orm, session):
        retrieved_alert = DeadlineAlert.get_by_id(deadline_alert_orm.id, session=session)
        assert retrieved_alert.id == deadline_alert_orm.id
        assert retrieved_alert.matches_definition(deadline_alert_orm)

    def test_deadline_alert_get_by_id_not_found(self, session):
        from sqlalchemy.exc import NoResultFound

        nonexistent_uuid = "00000000-0000-7000-8000-000000000000"
        with pytest.raises(NoResultFound, match="No DeadlineAlert found"):
            DeadlineAlert.get_by_id(nonexistent_uuid, session=session)

    def test_serialized_custom_reference_kwargs_handling(self):
        """Test that SerializedCustomReference properly filters and validates kwargs."""

        class StrictCustomRef(BaseDeadlineReference):
            reference_name = "StrictCustomRef"
            required_kwargs = {"dag_id", "run_id"}

            def _evaluate_with(self, *, session, dag_id, run_id):
                return timezone.utcnow()

        inner_ref = StrictCustomRef()
        inner_ref._evaluate_with = Mock(return_value=timezone.utcnow())

        wrapper = SerializedReferenceModels.SerializedCustomReference(inner_ref)

        wrapper.evaluate_with(
            session=None,
            interval=timedelta(hours=1),
            dag_id="test_dag",
            run_id="test_run",
            extra_param="should_be_filtered",
        )

        inner_ref._evaluate_with.assert_called_once_with(session=None, dag_id="test_dag", run_id="test_run")

        # try calling with missing required parameters
        with pytest.raises(ValueError, match="missing required parameters: run_id"):
            wrapper.evaluate_with(
                session=None,
                interval=timedelta(hours=1),
                dag_id="test_dag",
            )

    def test_core_deadline_reference_treated_as_builtins(self):
        """Test that refs from airflow.models.deadline are still treated as builtins."""
        from airflow.models.deadline import ReferenceModels
        from airflow.serialization.encoders import encode_deadline_reference

        ref = ReferenceModels.DagRunLogicalDateDeadline()
        serialized = encode_deadline_reference(ref)

        assert "__class_path" not in serialized
        assert serialized["reference_type"] == "DagRunLogicalDateDeadline"

    def test_is_builtin_reference(self):
        """Test that is_builtin_reference correctly identifies built-in vs custom references."""
        assert SerializedReferenceModels.is_builtin_reference("DagRunLogicalDateDeadline") is True
        assert SerializedReferenceModels.is_builtin_reference("DagRunQueuedAtDeadline") is True
        assert SerializedReferenceModels.is_builtin_reference("AverageRuntimeDeadline") is True

        assert SerializedReferenceModels.is_builtin_reference("MyCustomRef") is False
