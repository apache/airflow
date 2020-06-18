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

from airflow.api_connexion.schemas.dagrun_schema import dagrun_collection_schema, dagrun_schema
from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_runs


class TestDAGRunBase(unittest.TestCase):

    def setUp(self) -> None:
        clear_db_runs()
        self.now = timezone.utcnow()

    def tearDown(self) -> None:
        clear_db_runs()


class TestDAGRunSchema(TestDAGRunBase):

    @provide_session
    def test_serialzie(self, session):

        dagrun_model = DagRun(run_id='my-dag-run',
                              run_type=DagRunType.MANUAL.value,
                              execution_date=self.now,
                              start_date=self.now,
                              conf='{"start": "stop"}'
                              )
        session.add(dagrun_model)
        session.commit()
        dagrun_model = session.query(DagRun).first()
        deserialized_dagrun = dagrun_schema.dump(dagrun_model)

        self.assertEqual(
            deserialized_dagrun[0],
            {
                'dag_id': "",
                'dag_run_id': 'my-dag-run',
                'end_date': '',
                'state': 'running',
                'execution_date': str(self.now.isoformat()),
                'external_trigger': True,
                'start_date': str(self.now.isoformat()),
                'conf': '{"start": "stop"}'
            }
        )

    def test_deserialize(self):
        # Only dag_run_id, execution_date, state,
        # and conf are loaded.
        # dag_run_id should be loaded as run_id
        serialized_dagrun = {
            'dag_id': "",
            'dag_run_id': 'my-dag-run',
            'end_date': '',
            'state': 'failed',
            'execution_date': str(self.now.isoformat()),
            'external_trigger': True,
            'start_date': str(self.now.isoformat()),
            'conf': '{"start": "stop"}'
        }

        result = dagrun_schema.load(serialized_dagrun)
        self.assertEqual(
            result.data,
            {
                'run_id': 'my-dag-run',
                'execution_date': self.now,
                'state': 'failed',
                'conf': '{"start": "stop"}'
            }
        )


class TestDagRunCollection(TestDAGRunBase):

    @provide_session
    def test_serialize(self, session):
        dagrun_model_1 = DagRun(
            run_id='my-dag-run',
            execution_date=self.now,
            run_type=DagRunType.MANUAL.value,
            start_date=self.now,
            conf='{"start": "stop"}'
        )
        dagrun_model_2 = DagRun(
            run_id='my-dag-run-2',
            execution_date=self.now,
            start_date=self.now,
            run_type=DagRunType.MANUAL.value,
        )
        dagruns = [dagrun_model_1, dagrun_model_2]
        session.add_all(dagruns)
        session.commit()
        deserialized_dagruns = dagrun_collection_schema.dump(dagruns)
        self.assertEqual(
            deserialized_dagruns.data,
            {
                'dag_runs': [
                    {
                        'dag_id': "",
                        'dag_run_id': 'my-dag-run',
                        'end_date': '',
                        'execution_date': str(self.now.isoformat()),
                        'external_trigger': True,
                        'state': 'running',
                        'start_date': str(self.now.isoformat()),
                        'conf': '{"start": "stop"}'
                    },
                    {
                        'dag_id': "",
                        'dag_run_id': 'my-dag-run-2',
                        'end_date': '',
                        'state': 'running',
                        'execution_date': str(self.now.isoformat()),
                        'external_trigger': True,
                        'start_date': str(self.now.isoformat()),
                        'conf': {}
                    }
                ],
                'total_entries': 2
            }
        )
