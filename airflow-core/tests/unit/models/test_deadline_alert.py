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

import pytest
import time_machine

from airflow.models.deadline import ReferenceModels
from airflow.models.deadline_alert import DeadlineAlert
from airflow.sdk.definitions.deadline import DeadlineReference

from tests_common.test_utils import db
from unit.models import DEFAULT_DATE

DEADLINE_NAME = "Test Alert"
DEADLINE_DESCRIPTION = "This is a test alert description"
DEADLINE_REFERENCE = DeadlineReference.DAGRUN_QUEUED_AT.serialize_reference()
DEADLINE_INTERVAL = 60
DEADLINE_CALLBACK = {"path": "test.callback"}
SERIALIZED_DAG_ID = "serialized_dag_uuid"


def _clean_db():
    db.clear_db_deadline_alert()


@pytest.fixture
def deadline_alert_orm(session):
    with time_machine.travel(DEFAULT_DATE, tick=False):
        alert = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            name=DEADLINE_NAME,
            description=DEADLINE_DESCRIPTION,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
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

    def test_minimal_deadline_alert_creation(self, session):
        with time_machine.travel(DEFAULT_DATE, tick=False):
            deadline_alert = DeadlineAlert(
                serialized_dag_id=SERIALIZED_DAG_ID,
                reference=DEADLINE_REFERENCE,
                interval=DEADLINE_INTERVAL,
                callback=DEADLINE_CALLBACK,
            )
            session.add(deadline_alert)
            session.flush()

            assert deadline_alert.id is not None
            assert deadline_alert.created_at == DEFAULT_DATE
            assert deadline_alert.name is None
            assert deadline_alert.description is None

    def test_deadline_alert_repr(self, deadline_alert_orm):
        assert all(
            value in repr(deadline_alert_orm)
            for value in [
                "[DeadlineAlert]",
                "id=",
                f"created_at={DEFAULT_DATE}",
                f"name={DEADLINE_NAME}",
                f"reference={DEADLINE_REFERENCE}",
                "interval=1m",
                f"callback={DEADLINE_CALLBACK}",
            ]
        )

    def test_deadline_alert_equality(self, session):
        alert1 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
        )
        alert2 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
        )
        assert alert1 == alert2

        different_ref = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DeadlineReference.DAGRUN_LOGICAL_DATE.serialize_reference(),
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
        )
        assert alert1 != different_ref

        different_interval = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=120,
            callback=DEADLINE_CALLBACK,
        )
        assert alert1 != different_interval

        different_callback = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback={"path": "different.callback"},
        )
        assert alert1 != different_callback

        assert alert1 != "not a deadline alert"

    def test_deadline_alert_hash(self, session):
        alert1 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
        )
        alert2 = DeadlineAlert(
            serialized_dag_id=SERIALIZED_DAG_ID,
            reference=DEADLINE_REFERENCE,
            interval=DEADLINE_INTERVAL,
            callback=DEADLINE_CALLBACK,
        )

        assert hash(alert1) == hash(alert2)

    def test_deadline_alert_reference_class_property(self, deadline_alert_orm):
        assert deadline_alert_orm.reference_class == ReferenceModels.DagRunQueuedAtDeadline

    def test_deadline_alert_get_by_id(self, deadline_alert_orm, session):
        retrieved_alert = DeadlineAlert.get_by_id(deadline_alert_orm.id, session=session)
        assert retrieved_alert == deadline_alert_orm

    def test_deadline_alert_get_by_id_not_found(self, session):
        from sqlalchemy.exc import NoResultFound

        with pytest.raises(NoResultFound, match="No row was found"):
            DeadlineAlert.get_by_id("nonexistent_uuid", session=session)


id
