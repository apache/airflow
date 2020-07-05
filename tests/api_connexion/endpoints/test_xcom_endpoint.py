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
from parameterized import parameterized

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
        """
        Setup For XCom endpoint TC
        """
        self.client = self.app.test_client()  # type:ignore
        # clear existing xcoms
        with create_session() as session:
            session.query(XCom).delete()
            session.query(DR).delete()

    def tearDown(self) -> None:
        """
        Clear Hanging XComs
        """
        with create_session() as session:
            session.query(XCom).delete()
            session.query(DR).delete()


class TestDeleteXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.delete(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T00:00:00Z/xcomEntries/XCOM_KEY"
        )
        assert response.status_code == 204


class TestGetXComEntry(TestXComEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        xcom_key = 'test-xcom-key'
        execution_date_parsed = parse_execution_date(execution_date)
        xcom_model = XCom(key=xcom_key,
                          execution_date=execution_date_parsed,
                          task_id=task_id,
                          dag_id=dag_id,
                          timestamp=execution_date_parsed)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        dagrun = DR(dag_id=dag_id,
                    run_id=dag_run_id,
                    execution_date=execution_date_parsed,
                    start_date=execution_date_parsed,
                    run_type=DagRunType.MANUAL.value)
        session.add(xcom_model)
        session.add(dagrun)
        session.commit()
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
        )
        self.assertEqual(200, response.status_code)
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
    @provide_session
    def test_should_response_200(self, session):
        dag_id = 'test-dag-id'
        task_id = 'test-task-id'
        execution_date = '2005-04-02T00:00:00+00:00'
        execution_date_parsed = parse_execution_date(execution_date)
        xcom_model_1 = XCom(key='test-xcom-key-1',
                            execution_date=execution_date_parsed,
                            task_id=task_id,
                            dag_id=dag_id,
                            timestamp=execution_date_parsed)
        xcom_model_2 = XCom(key='test-xcom-key-2',
                            execution_date=execution_date_parsed,
                            task_id=task_id,
                            dag_id=dag_id,
                            timestamp=execution_date_parsed)
        dag_run_id = DR.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        dagrun = DR(dag_id=dag_id,
                    run_id=dag_run_id,
                    execution_date=execution_date_parsed,
                    start_date=execution_date_parsed,
                    run_type=DagRunType.MANUAL.value)
        xcom_models = [xcom_model_1, xcom_model_2]
        session.add_all(xcom_models)
        session.add(dagrun)
        session.commit()
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries"
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            response.json,
            {
                'xcom_entries': [
                    {
                        'dag_id': dag_id,
                        'execution_date': execution_date,
                        'key': 'test-xcom-key-1',
                        'task_id': task_id,
                        'timestamp': execution_date
                    },
                    {
                        'dag_id': dag_id,
                        'execution_date': execution_date,
                        'key': 'test-xcom-key-2',
                        'task_id': task_id,
                        'timestamp': execution_date
                    }
                ],
                'total_entries': 2,
            }
        )


class TestPaginationGetXComEntries(TestXComEndpoint):

    def setUp(self):
        super().setUp()
        self.dag_id = 'test-dag-id'
        self.task_id = 'test-task-id'
        self.execution_date = '2005-04-02T00:00:00+00:00'
        self.execution_date_parsed = parse_execution_date(self.execution_date)
        self.dag_run_id = DR.generate_run_id(DagRunType.MANUAL, self.execution_date_parsed)

    @parameterized.expand(
        [
            (
                "limit=1",
                ["TEST_XCOM_KEY1"],
            ),
            (
                "limit=2",
                ["TEST_XCOM_KEY1", "TEST_XCOM_KEY10"],
            ),
            (
                "offset=5",
                [
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ]
            ),
            (
                "offset=0",
                [
                    "TEST_XCOM_KEY1",
                    "TEST_XCOM_KEY10",
                    "TEST_XCOM_KEY2",
                    "TEST_XCOM_KEY3",
                    "TEST_XCOM_KEY4",
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9"
                ]
            ),
            (
                "limit=1&offset=5",
                ["TEST_XCOM_KEY5"],
            ),
            (
                "limit=1&offset=1",
                ["TEST_XCOM_KEY10"],
            ),
            (
                "limit=2&offset=2",
                ["TEST_XCOM_KEY2", "TEST_XCOM_KEY3"],
            ),
        ]
    )
    @provide_session
    def test_handle_limit_offset(self, query_params, expected_xcom_ids, session):
        url = "/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries?{query_params}"
        url = url.format(dag_id=self.dag_id,
                         dag_run_id=self.dag_run_id,
                         task_id=self.task_id,
                         query_params=query_params)
        dagrun = DR(dag_id=self.dag_id,
                    run_id=self.dag_run_id,
                    execution_date=self.execution_date_parsed,
                    start_date=self.execution_date_parsed,
                    run_type=DagRunType.MANUAL.value)
        xcom_models = self._create_xcoms(10)
        session.add_all(xcom_models)
        session.add(dagrun)
        session.commit()
        response = self.client.get(url)
        assert response.status_code == 200
        self.assertEqual(response.json["total_entries"], 10)
        conn_ids = [conn["key"] for conn in response.json["xcom_entries"] if conn]
        self.assertEqual(conn_ids, expected_xcom_ids)

    def _create_xcoms(self, count):
        return [XCom(
            key=f'TEST_XCOM_KEY{i}',
            execution_date=self.execution_date_parsed,
            task_id=self.task_id,
            dag_id=self.dag_id,
            timestamp=self.execution_date_parsed,
        ) for i in range(1, count + 1)]


class TestPatchXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T00:00:00Z/xcomEntries"
        )
        assert response.status_code == 200


class TestPostXComEntry(TestXComEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post(
            "/dags/TEST_DAG_ID/taskInstances/TEST_TASK_ID/2005-04-02T00:00:00Z/xcomEntries/XCOM_KEY"
        )
        assert response.status_code == 200
