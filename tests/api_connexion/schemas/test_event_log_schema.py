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

import unittest

from airflow import DAG
from airflow.api_connexion.schemas.event_log_schema import (
    EventLogCollection,
    event_log_collection_schema,
    event_log_schema,
)
from airflow.models import Log, TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session


class TestEventLogSchemaBase(unittest.TestCase):
    def setUp(self) -> None:
        with create_session() as session:
            session.query(Log).delete()
        self.default_time = "2020-06-09T13:00:00+00:00"
        self.default_time2 = '2020-06-11T07:00:00+00:00'

    def tearDown(self) -> None:
        with create_session() as session:
            session.query(Log).delete()

    def _create_task_instance(self):
        with DAG(
            'TEST_DAG_ID',
            start_date=timezone.parse(self.default_time),
            end_date=timezone.parse(self.default_time),
        ):
            op1 = DummyOperator(task_id="TEST_TASK_ID", owner="airflow")
        return TaskInstance(task=op1, execution_date=timezone.parse(self.default_time))


class TestEventLogSchema(TestEventLogSchemaBase):
    @provide_session
    def test_serialize(self, session):
        event_log_model = Log(event="TEST_EVENT", task_instance=self._create_task_instance())
        session.add(event_log_model)
        session.commit()
        event_log_model.dttm = timezone.parse(self.default_time)
        log_model = session.query(Log).first()
        deserialized_log = event_log_schema.dump(log_model)
        self.assertEqual(
            deserialized_log,
            {
                "event_log_id": event_log_model.id,
                "event": "TEST_EVENT",
                "dag_id": "TEST_DAG_ID",
                "task_id": "TEST_TASK_ID",
                "execution_date": self.default_time,
                "owner": 'airflow',
                "when": self.default_time,
                "extra": None,
            },
        )


class TestEventLogCollection(TestEventLogSchemaBase):
    @provide_session
    def test_serialize(self, session):
        event_log_model_1 = Log(event="TEST_EVENT_1", task_instance=self._create_task_instance())
        event_log_model_2 = Log(event="TEST_EVENT_2", task_instance=self._create_task_instance())
        event_logs = [event_log_model_1, event_log_model_2]
        session.add_all(event_logs)
        session.commit()
        event_log_model_1.dttm = timezone.parse(self.default_time)
        event_log_model_2.dttm = timezone.parse(self.default_time2)
        instance = EventLogCollection(event_logs=event_logs, total_entries=2)
        deserialized_event_logs = event_log_collection_schema.dump(instance)
        self.assertEqual(
            deserialized_event_logs,
            {
                "event_logs": [
                    {
                        "event_log_id": event_log_model_1.id,
                        "event": "TEST_EVENT_1",
                        "dag_id": "TEST_DAG_ID",
                        "task_id": "TEST_TASK_ID",
                        "execution_date": self.default_time,
                        "owner": 'airflow',
                        "when": self.default_time,
                        "extra": None,
                    },
                    {
                        "event_log_id": event_log_model_2.id,
                        "event": "TEST_EVENT_2",
                        "dag_id": "TEST_DAG_ID",
                        "task_id": "TEST_TASK_ID",
                        "execution_date": self.default_time,
                        "owner": 'airflow',
                        "when": self.default_time2,
                        "extra": None,
                    },
                ],
                "total_entries": 2,
            },
        )
