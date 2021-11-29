#
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

from pytest_httpx import HTTPXMock

from airflow.api.client.stable_api_client import AirflowClient


class TestStableAPIClient:
    def setup_method(self):
        self.client = AirflowClient(api_base_url='http://localhost:8080', auth=None)
        self.base_url = 'http://localhost:8080/api/v1'

    def test_trigger_dag(self, httpx_mock: HTTPXMock):
        test_dag_id = "example_bash_operator"
        run_id = "Test_Run_ID"
        data = {"dag_run_id": run_id, "dag_id": test_dag_id, "conf": {}}
        httpx_mock.add_response(url=f'{self.base_url}/dags/{test_dag_id}/dagRuns', json=data, method='POST')

        # no execution date, execution date should be set automatically
        assert self.client.trigger_dag(dag_id=test_dag_id) == data
        assert self.client.trigger_dag(dag_id=test_dag_id, run_id=run_id) == data

    def test_delete_dag(self, httpx_mock: HTTPXMock):
        test_dag_id = "example_bash_operator"
        text = f"DAG with dag_id: {test_dag_id} deleted"
        httpx_mock.add_response(
            url=f'{self.base_url}/dags/{test_dag_id}', method='DELETE', status_code=204, text=text
        )
        assert self.client.delete_dag(dag_id=test_dag_id) == text

    def test_get_pool(self, httpx_mock: HTTPXMock):
        name = "test_pool"
        data = {'name': name, 'slots': 1, 'description': 'test pool'}
        httpx_mock.add_response(url=f'{self.base_url}/pools/{name}', json=data, method='GET')
        assert self.client.get_pool(name=name) == data

    def test_get_pools(self, httpx_mock: HTTPXMock):
        name = "test_pool"
        data = [{'name': name, 'slots': 1, 'description': 'test pool'}]
        httpx_mock.add_response(url=f'{self.base_url}/pools', json=data, method='GET')
        assert self.client.get_pools() == data

    def test_create_pool(self, httpx_mock: HTTPXMock):
        name = "test_pool"
        data = {'name': name, 'slots': 1, 'description': 'test pool'}
        httpx_mock.add_response(url=f'{self.base_url}/pools', json=data, method='POST')
        assert self.client.create_pool(name=name, slots=1, description='test pool') == data

    def test_delete_pool(self, httpx_mock: HTTPXMock):
        name = "test_pool"
        text = f"Pool with name: {name} deleted"
        httpx_mock.add_response(
            url=f'{self.base_url}/pools/{name}', method='DELETE', status_code=204, text=text
        )
        assert self.client.delete_pool(name=name) == text

    def test_lineage(self, httpx_mock: HTTPXMock):
        dag_id = "example_bash_operator"
        dag_run_id = "test_run_id"
        httpx_mock.add_response(
            url=f'{self.base_url}/dags/{dag_id}/dagRuns/{dag_run_id}/lineage', json={}, method='GET'
        )
        assert self.client.get_lineage(dag_id=dag_id, run_id=dag_run_id) == {}
