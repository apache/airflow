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
"""JSON API Client"""

from urllib.parse import urljoin

from airflow.api.client import api_client


class Client(api_client.Client):
    """Json API client implementation."""

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

        return resp.json()

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        endpoint = f'/api/experimental/dags/{dag_id}/dag_runs'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(
            url,
            method='POST',
            json={
                "run_id": run_id,
                "conf": conf,
                "execution_date": execution_date,
            },
        )
        return data['message']

    def delete_dag(self, dag_id):
        endpoint = f'/api/experimental/dags/{dag_id}/delete_dag'
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url, method='DELETE')
        return data['message']

    def get_pool(self, name):
        endpoint = f'/api/experimental/pools/{name}'
        url = urljoin(self._api_base_url, endpoint)
        pool = self._request(url)
        return pool['pool'], pool['slots'], pool['description']

    def get_pools(self):
        endpoint = '/api/experimental/pools'
        url = urljoin(self._api_base_url, endpoint)
        pools = self._request(url)
        return [(p['pool'], p['slots'], p['description']) for p in pools]

    def create_pool(self, name, slots, description):
        endpoint = '/api/experimental/pools'
        url = urljoin(self._api_base_url, endpoint)
        pool = self._request(
            url,
            method='POST',
            json={
                'name': name,
                'slots': slots,
                'description': description,
            },
        )
        return pool['pool'], pool['slots'], pool['description']

    def delete_pool(self, name):
        endpoint = f'/api/experimental/pools/{name}'
        url = urljoin(self._api_base_url, endpoint)
        pool = self._request(url, method='DELETE')
        return pool['pool'], pool['slots'], pool['description']

    def get_lineage(self, dag_id: str, execution_date: str):
        endpoint = f"/api/experimental/lineage/{dag_id}/{execution_date}"
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url, method='GET')
        return data['message']
