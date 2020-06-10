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

import pytest

from airflow.models import DagRun as DR, XCom
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session, provide_session
from airflow.utils.types import DagRunType
from airflow.www import app


class TestXComEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        # clear existing xcoms
        with create_session() as session:
            session.query(XCom).delete()
            session.query(DR).delete()


class TestDeleteXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.delete(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomEntries/XCOM_KEY"
        )
        assert response.status_code == 204


class TestGetXComEntry(TestXComEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        # WIP datetime spece
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T21:37:42+00:00'
        xcom_key = 'test-xcom-key'
        execution_date_parsed = parse_execution_date(execution_date)
        xcom_model = XCom(
            key=xcom_key,
            execution_date=execution_date_parsed,
            task_id=task_id,
            dag_id=dag_id,
            timestamp=execution_date_parsed,
        )
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        dagrun = DR(
            dag_id=dag_id,
            run_id=dag_run_id,
            execution_date=execution_date_parsed,
            start_date=execution_date_parsed,
            run_type=DagRunType.MANUAL.value,
        )
        session.add(xcom_model)
        session.add(dagrun)
        session.commit()
        response = self.client.get(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
        )
        self.assertEqual(200, response.status_code)
        print(response.json)
        self.assertEqual(
            response.json,
            {
                'dag_id': dag_id,
                'execution_date': execution_date,
                'key': xcom_key,
                'task_id': task_id,
                'timestamp': execution_date
            }
        )


class TestGetXComEntries(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomEntries/"
        )
        assert response.status_code == 200


class TestPatchXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomEntries"
        )
        assert response.status_code == 200


class TestPostXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T21:37:42Z/xcomEntries/XCOM_KEY"
        )
        assert response.status_code == 200
