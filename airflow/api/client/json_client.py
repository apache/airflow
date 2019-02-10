# -*- coding: utf-8 -*-
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

from future.moves.urllib.parse import urljoin
import requests

from airflow.api.client import api_client


class Client(api_client.Client):
    """Json API client implementation."""

    def _request(self, endpoint, method='GET', json=None):
        url = urljoin(self._api_base_url, endpoint)
        params = {
            'url': url,
            'auth': self._auth,
        }
        if json is not None:
            params['json'] = json

        resp = getattr(requests, method.lower())(**params)
        if not resp.ok:
            try:
                data = resp.json()
            except Exception:
                data = {}
            raise IOError(data.get('error', 'Server error'))

        return resp.json()

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None):
        endpoint = '/api/experimental/dags/{}/dag_runs'.format(dag_id)
        data = self._request(endpoint, method='POST',
                             json={
                                 "run_id": run_id,
                                 "conf": conf,
                                 "execution_date": execution_date,
                             })
        return data['message']

    def delete_dag(self, dag_id):
        endpoint = '/api/experimental/dags/{}/delete_dag'.format(dag_id)
        data = self._request(endpoint, method='DELETE')
        return data['message']

    def get_pool(self, name):
        endpoint = '/api/experimental/pools/{}'.format(name)
        pool = self._request(endpoint)
        return pool['pool'], pool['slots'], pool['description']

    def get_pools(self):
        endpoint = '/api/experimental/pools'
        pools = self._request(endpoint)
        return [(p['pool'], p['slots'], p['description']) for p in pools]

    def create_pool(self, name, slots, description):
        endpoint = '/api/experimental/pools'
        pool = self._request(endpoint, method='POST',
                             json={
                                 'name': name,
                                 'slots': slots,
                                 'description': description,
                             })
        return pool['pool'], pool['slots'], pool['description']

    def delete_pool(self, name):
        endpoint = '/api/experimental/pools/{}'.format(name)
        pool = self._request(endpoint, method='DELETE')
        return pool['pool'], pool['slots'], pool['description']

    def add_connection(self, conn_id,
                       conn_uri=None,
                       conn_type=None,
                       conn_host=None,
                       conn_login=None,
                       conn_password=None,
                       conn_schema=None,
                       conn_port=None,
                       conn_extra=None):
        endpoint = '/api/experimental/connections'
        conn = self._request(endpoint, method='POST', json={
            'conn_id': conn_id,
            'conn_uri': conn_uri,
            'conn_type': conn_type,
            'conn_host': conn_host,
            'conn_login': conn_login,
            'conn_password': conn_password,
            'conn_schema': conn_schema,
            'conn_port': conn_port,
            'conn_extra': conn_extra})
        return conn.to_json()

    def delete_connection(self, conn_id, delete_all=False):
        """

        :param conn_id:
        :param delete_all:
        :return: the conn_id(s) of the Connection(s) that were removed
        """
        endpoint = '/api/experimental/connections/{}'.format(conn_id)
        conn_id = self._request(endpoint, method='DELETE', json={'delete_all': delete_all})
        return conn_id

    def list_connections(self):
        """

        :return: All Connections
        """
        endpoint = '/api/experimental/connections'
        conns = self._request(endpoint)
        return conns

    def update_connection(self, conn_id,
                          conn_uri=None,
                          conn_type=None,
                          conn_host=None,
                          conn_login=None,
                          conn_password=None,
                          conn_schema=None,
                          conn_port=None,
                          conn_extra=None):
        """

        :param conn_id:
        :param conn_uri:
        :param conn_type:
        :param conn_host:
        :param conn_login:
        :param conn_password:
        :param conn_schema:
        :param conn_port:
        :param conn_extra:
        :return: The updated Connection
        """
        endpoint = '/api/experimental/connections/'
        conn = self._request(endpoint, method='PATCH', json={
            'conn_id': conn_id,
            'conn_uri': conn_uri,
            'conn_type': conn_type,
            'conn_host': conn_host,
            'conn_login': conn_login,
            'conn_password': conn_password,
            'conn_schema': conn_schema,
            'conn_port': conn_port,
            'conn_extra': conn_extra})
        return conn.to_json()
