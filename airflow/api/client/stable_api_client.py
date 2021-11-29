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

from json.decoder import JSONDecodeError
from typing import Optional
from urllib.parse import urljoin

from airflow.api.client import api_client
from airflow.models import DagRun
from airflow.utils.state import State


class AirflowClient(api_client.Client):
    """API Client using the stable REST API"""

    def _request(self, url, method='GET', json=None):
        params = {
            'url': url,
        }
        if json is not None:
            params['json'] = json
        resp = getattr(self._session, method.lower())(**params)
        if resp.is_error:
            # It is justified here because there might be many resp types.
            try:
                data = resp.json()
            except Exception:
                data = {}
            raise OSError(data.get('error', 'Server error'))
        try:  # DELETE requests doesn't return a value
            return resp.json()
        except JSONDecodeError:
            return resp.text

    def trigger_dag(
        self, *, dag_id, run_id=None, conf=None, execution_date=None, logical_date=None, state=State.QUEUED
    ):
        endpoint = f'/api/v1/dags/{dag_id}/dagRuns'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(
            url,
            method='POST',
            json={
                "dag_run_id": run_id,
                "conf": conf or {},
                "execution_date": execution_date,
                "state": state,
                "logical_date": logical_date,
            },
        )
        return data

    def delete_dag(self, dag_id):
        endpoint = f'/api/v1/dags/{dag_id}'
        url = urljoin(self._api_base_url, endpoint)
        self._request(url, method='DELETE')
        return f"DAG with dag_id: {dag_id} deleted"

    def get_pool(self, name):
        endpoint = f'/api/v1/pools/{name}'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url)
        return data

    def get_pools(self):
        endpoint = '/api/v1/pools'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url)
        return data

    def create_pool(self, name, slots, description):
        endpoint = '/api/v1/pools'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(
            url,
            method='POST',
            json={
                'name': name,
                'slots': slots,
                'description': description,
            },
        )
        return data

    def delete_pool(self, name):
        endpoint = f'/api/v1/pools/{name}'
        url = urljoin(self._api_base_url, endpoint)
        self._request(url, method='DELETE')
        return f"Pool with name: {name} deleted"

    def get_lineage(self, dag_id: str, execution_date: Optional[str] = None, run_id: Optional[str] = None):
        if execution_date:
            dr = DagRun.find(dag_id=dag_id, execution_date=execution_date, run_id=run_id)
            if not dr:
                raise ValueError(f"DagRun with dag_id={dag_id}, execution_date={execution_date} not found ")
            run_id = dr.run_id
        endpoint = f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/lineage"
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url, method='GET')
        return data
