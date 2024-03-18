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
"""Client for all the API clients."""

from __future__ import annotations

import httpx


class Client:
    """Base API client for all API clients."""

    def __init__(self, api_base_url, auth=None, session: httpx.Client | None = None):
        self._api_base_url = api_base_url
        self._session: httpx.Client = session or httpx.Client()
        if auth:
            self._session.auth = auth

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None, replace_microseconds=True):
        """Create a dag run for the specified dag.

        :param dag_id:
        :param run_id:
        :param conf:
        :param execution_date:
        :param replace_microseconds:
        :return:
        """
        raise NotImplementedError()

    def delete_dag(self, dag_id):
        """Delete all DB records related to the specified dag.

        :param dag_id:
        """
        raise NotImplementedError()

    def get_pool(self, name):
        """Get pool.

        :param name: pool name
        """
        raise NotImplementedError()

    def get_pools(self):
        """Get all pools."""
        raise NotImplementedError()

    def create_pool(self, name, slots, description, include_deferred):
        """Create a pool.

        :param name: pool name
        :param slots: pool slots amount
        :param description: pool description
        :param include_deferred: include deferred tasks in pool calculations
        """
        raise NotImplementedError()

    def delete_pool(self, name):
        """Delete pool.

        :param name: pool name
        """
        raise NotImplementedError()

    def get_lineage(self, dag_id: str, execution_date: str):
        """
        Return the lineage information for the dag on this execution date.

        :param dag_id:
        :param execution_date:
        :return:
        """
        raise NotImplementedError()
