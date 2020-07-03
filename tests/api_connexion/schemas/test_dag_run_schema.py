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

from dateutil.parser import parse
from marshmallow import ValidationError

from airflow.api_connexion.schemas.dag_run_schema import (
    DAGRunCollection, dagrun_collection_schema, dagrun_schema,
)
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs


class TestDAGRunBase(unittest.TestCase):

    def setUp(self) -> None:
        clear_db_runs()
        self.default_time = "2020-06-09T13:59:56.336000+00:00"

    def tearDown(self) -> None:
        clear_db_runs()


class TestDAGRunSchema(TestDAGRunBase):

    @provide_session
    def test_serialze(self, session):
        dagrun_model = DagRun(run_id='my-dag-run',
                              run_type=DagRunType.MANUAL.value,
                              execution_date=timezone.parse(self.default_time),
                              start_date=timezone.parse(self.default_time),
                              conf='{"start": "stop"}'
                              )
        session.add(dagrun_model)
        session.commit()
        dagrun_model = session.query(DagRun).first()
        deserialized_dagrun = dagrun_schema.dump(dagrun_model)

        self.assertEqual(
            deserialized_dagrun,
            {
                'dag_id': None,
                'dag_run_id': 'my-dag-run',
                'end_date': None,
                'state': 'running',
                'execution_date': self.default_time,
                'external_trigger': True,
                'start_date': self.default_time,
                'conf': {"start": "stop"}
            }
        )

    def test_deserialize(self):
        # Only dag_run_id, execution_date, state,
        # and conf are loaded.
        # dag_run_id should be loaded as run_id
        serialized_dagrun = {
            'dag_run_id': 'my-dag-run',
            'state': 'failed',
            'execution_date': self.default_time,
            'conf': '{"start": "stop"}'
        }

        result = dagrun_schema.load(serialized_dagrun)
        self.assertEqual(
            result,
            {
                'run_id': 'my-dag-run',
                'execution_date': parse(self.default_time),
                'state': 'failed',
                'conf': {"start": "stop"}
            }
        )

    def test_deserialize_2(self):
        # loading dump_only field raises
        serialized_dagrun = {
            'dag_id': None,
            'dag_run_id': 'my-dag-run',
            'end_date': None,
            'state': 'failed',
            'execution_date': self.default_time,
            'external_trigger': True,
            'start_date': self.default_time,
            'conf': {"start": "stop"}
        }
        with self.assertRaises(ValidationError):
            dagrun_schema.load(serialized_dagrun)


class TestDagRunCollection(TestDAGRunBase):

    @provide_session
    def test_serialize(self, session):
        dagrun_model_1 = DagRun(
            run_id='my-dag-run',
            execution_date=timezone.parse(self.default_time),
            run_type=DagRunType.MANUAL.value,
            start_date=timezone.parse(self.default_time),
            conf='{"start": "stop"}'
        )
        dagrun_model_2 = DagRun(
            run_id='my-dag-run-2',
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            run_type=DagRunType.MANUAL.value,
        )
        dagruns = [dagrun_model_1, dagrun_model_2]
        session.add_all(dagruns)
        session.commit()
        instance = DAGRunCollection(dag_runs=dagruns,
                                    total_entries=2)
        deserialized_dagruns = dagrun_collection_schema.dump(instance)
        self.assertEqual(
            deserialized_dagruns,
            {
                'dag_runs': [
                    {
                        'dag_id': None,
                        'dag_run_id': 'my-dag-run',
                        'end_date': None,
                        'execution_date': self.default_time,
                        'external_trigger': True,
                        'state': 'running',
                        'start_date': self.default_time,
                        'conf': {"start": "stop"}
                    },
                    {
                        'dag_id': None,
                        'dag_run_id': 'my-dag-run-2',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': self.default_time,
                        'external_trigger': True,
                        'start_date': self.default_time,
                        'conf': {}
                    }
                ],
                'total_entries': 2
            }
        )
