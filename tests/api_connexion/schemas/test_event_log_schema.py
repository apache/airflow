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

from airflow.api_connexion.schemas.event_log_schema import (
    EventLogCollection,
    event_log_collection_schema,
    event_log_schema,
)
from airflow.models import Log
from airflow.utils import timezone

pytestmark = pytest.mark.db_test


@pytest.fixture
def task_instance(session, create_task_instance, request):
    return create_task_instance(
        session=session,
        dag_id="TEST_DAG_ID",
        task_id="TEST_TASK_ID",
        execution_date=request.instance.default_time,
    )


class TestEventLogSchemaBase:
    @pytest.fixture(autouse=True)
    def set_attrs(self):
        self.default_time = timezone.parse("2020-06-09T13:00:00+00:00")
        self.default_time2 = timezone.parse("2020-06-11T07:00:00+00:00")


class TestEventLogSchema(TestEventLogSchemaBase):
    def test_serialize(self, task_instance):
        event_log_model = Log(event="TEST_EVENT", task_instance=task_instance)
        event_log_model.dttm = self.default_time
        deserialized_log = event_log_schema.dump(event_log_model)
        assert deserialized_log == {
            "event_log_id": event_log_model.id,
            "event": "TEST_EVENT",
            "dag_id": "TEST_DAG_ID",
            "task_id": "TEST_TASK_ID",
            "execution_date": self.default_time.isoformat(),
            "owner": "airflow",
            "when": self.default_time.isoformat(),
            "extra": None,
        }


class TestEventLogCollection(TestEventLogSchemaBase):
    def test_serialize(self, task_instance):
        event_log_model_1 = Log(event="TEST_EVENT_1", task_instance=task_instance)
        event_log_model_2 = Log(event="TEST_EVENT_2", task_instance=task_instance)
        event_logs = [event_log_model_1, event_log_model_2]
        event_log_model_1.dttm = self.default_time
        event_log_model_2.dttm = self.default_time2
        instance = EventLogCollection(event_logs=event_logs, total_entries=2)
        deserialized_event_logs = event_log_collection_schema.dump(instance)
        assert deserialized_event_logs == {
            "event_logs": [
                {
                    "event_log_id": event_log_model_1.id,
                    "event": "TEST_EVENT_1",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "execution_date": self.default_time.isoformat(),
                    "owner": "airflow",
                    "when": self.default_time.isoformat(),
                    "extra": None,
                },
                {
                    "event_log_id": event_log_model_2.id,
                    "event": "TEST_EVENT_2",
                    "dag_id": "TEST_DAG_ID",
                    "task_id": "TEST_TASK_ID",
                    "execution_date": self.default_time.isoformat(),
                    "owner": "airflow",
                    "when": self.default_time2.isoformat(),
                    "extra": None,
                },
            ],
            "total_entries": 2,
        }
