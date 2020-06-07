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

from airflow.www import app
from airflow.utils.session import provide_session, create_session
from airflow.models import XCom
from airflow.utils.dates import parse_execution_date


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
        dag_id = 'A'
        task_id = 'A'
        execution_date = '2005-04-02T21:37:42Z'
        xcom_key = 'A'
        xcom_model = XCom(
            key=xcom_key,
            execution_date=parse_execution_date(execution_date),
            task_id=task_id,
            dag_id=dag_id,
            timestamp=parse_execution_date(execution_date),
        )
        session.add(xcom_model)
        session.commit()
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/taskInstances/{task_id}/{execution_date}/xcomEntries/{xcom_key}"
        )
        assert response.status_code == 200
        print(response.json)
        self.assertEqual(
            response.json,
            {
                'key': xcom_key,
                'execution_date': execution_date,
                'task_id': task_id,
                'dag_id': dag_id,
                'timestamp': execution_date,
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
