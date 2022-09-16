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

import unittest

import pytest
from dateutil.parser import parse
from parameterized import parameterized

from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.schemas.dag_run_schema import (
    DAGRunCollection,
    dagrun_collection_schema,
    dagrun_schema,
)
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs

DEFAULT_TIME = "2020-06-09T13:59:56.336000+00:00"

SECOND_TIME = "2020-06-10T13:59:56.336000+00:00"


class TestDAGRunBase(unittest.TestCase):
    def setUp(self) -> None:
        clear_db_runs()
        self.default_time = DEFAULT_TIME
        self.second_time = SECOND_TIME

    def tearDown(self) -> None:
        clear_db_runs()


class TestDAGRunSchema(TestDAGRunBase):
    @provide_session
    def test_serialize(self, session):
        dagrun_model = DagRun(
            dag_id="my-dag-run",
            run_id="my-dag-run",
            state='running',
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            conf='{"start": "stop"}',
        )
        session.add(dagrun_model)
        session.commit()
        dagrun_model = session.query(DagRun).first()
        deserialized_dagrun = dagrun_schema.dump(dagrun_model)

        assert deserialized_dagrun == {
            "dag_id": "my-dag-run",
            "dag_run_id": "my-dag-run",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {"start": "stop"},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
        }

    @parameterized.expand(
        [
            (  # Conf not provided
                {"dag_run_id": "my-dag-run", "execution_date": DEFAULT_TIME},
                {"run_id": "my-dag-run", "execution_date": parse(DEFAULT_TIME)},
            ),
            (
                {
                    "dag_run_id": "my-dag-run",
                    "execution_date": DEFAULT_TIME,
                    "conf": {"start": "stop"},
                },
                {
                    "run_id": "my-dag-run",
                    "execution_date": parse(DEFAULT_TIME),
                    "conf": {"start": "stop"},
                },
            ),
            (
                {
                    "dag_run_id": "my-dag-run",
                    "execution_date": DEFAULT_TIME,
                    "conf": '{"start": "stop"}',
                },
                {
                    "run_id": "my-dag-run",
                    "execution_date": parse(DEFAULT_TIME),
                    "conf": {"start": "stop"},
                },
            ),
        ]
    )
    def test_deserialize(self, serialized_dagrun, expected_result):
        result = dagrun_schema.load(serialized_dagrun)
        assert result == expected_result

    def test_autofill_fields(self):
        """Dag_run_id and execution_date fields are autogenerated if missing"""
        serialized_dagrun = {}
        result = dagrun_schema.load(serialized_dagrun)
        assert result == {"execution_date": result["execution_date"], "run_id": result["run_id"]}

    def test_invalid_execution_date_raises(self):
        serialized_dagrun = {"execution_date": "mydate"}
        with pytest.raises(BadRequest) as ctx:
            dagrun_schema.load(serialized_dagrun)
        assert str(ctx.value) == "Incorrect datetime argument"


class TestDagRunCollection(TestDAGRunBase):
    @provide_session
    def test_serialize(self, session):
        dagrun_model_1 = DagRun(
            dag_id="my-dag-run",
            run_id="my-dag-run",
            state='running',
            execution_date=timezone.parse(self.default_time),
            run_type=DagRunType.MANUAL.value,
            start_date=timezone.parse(self.default_time),
            conf='{"start": "stop"}',
        )
        dagrun_model_2 = DagRun(
            dag_id="my-dag-run",
            run_id="my-dag-run-2",
            state='running',
            execution_date=timezone.parse(self.second_time),
            start_date=timezone.parse(self.default_time),
            run_type=DagRunType.MANUAL.value,
        )
        dagruns = [dagrun_model_1, dagrun_model_2]
        session.add_all(dagruns)
        session.commit()
        instance = DAGRunCollection(dag_runs=dagruns, total_entries=2)
        deserialized_dagruns = dagrun_collection_schema.dump(instance)
        assert deserialized_dagruns == {
            "dag_runs": [
                {
                    "dag_id": "my-dag-run",
                    "dag_run_id": "my-dag-run",
                    "end_date": None,
                    "execution_date": self.default_time,
                    "logical_date": self.default_time,
                    "external_trigger": True,
                    "state": "running",
                    "start_date": self.default_time,
                    "conf": {"start": "stop"},
                    "data_interval_end": None,
                    "data_interval_start": None,
                    "last_scheduling_decision": None,
                    "run_type": "manual",
                },
                {
                    "dag_id": "my-dag-run",
                    "dag_run_id": "my-dag-run-2",
                    "end_date": None,
                    "state": "running",
                    "execution_date": self.second_time,
                    "logical_date": self.second_time,
                    "external_trigger": True,
                    "start_date": self.default_time,
                    "conf": {},
                    "data_interval_end": None,
                    "data_interval_start": None,
                    "last_scheduling_decision": None,
                    "run_type": "manual",
                },
            ],
            "total_entries": 2,
        }
