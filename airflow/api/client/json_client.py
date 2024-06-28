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
"""JSON API Client."""

from __future__ import annotations

from urllib.parse import urljoin

from airflow.api.client import api_client


class Client(api_client.Client):
    """
    Json API client implementation.

    This client is used to interact with a Json API server and perform various actions
    such as triggering DAG runs,deleting DAGs, interacting with pools, and getting lineage information.
    """

    def _request(self, url: str, json=None, method: str = "GET") -> dict:
        """
        Make a request to the Json API server.

        :param url: The URL to send the request to.
        :param method: The HTTP method to use (e.g. "GET", "POST", "DELETE").
        :param json: A dictionary containing JSON data to send in the request body.
        :return: A dictionary containing the JSON response from the server.
        :raises OSError: If the server returns an error status.
        """
        params = {
            "url": url,
        }
        if json is not None:
            params["json"] = json
        resp = getattr(self._session, method.lower())(**params)
        if resp.is_error:
            # It is justified here because there might be many resp types.
            try:
                data = resp.json()
            except Exception:
                data = {}
            raise OSError(data.get("error", "Server error"))
        return resp.json()

    def trigger_dag(self, dag_id, run_id=None, conf=None, execution_date=None, replace_microseconds=True):
        """
        Trigger a DAG run.

        :param dag_id: The ID of the DAG to trigger.
        :param run_id: The ID of the DAG run to create. If not provided, a default ID will be generated.
        :param conf: A dictionary containing configuration data to pass to the DAG run.
        :param execution_date: The execution date for the DAG run, in the format "YYYY-MM-DDTHH:MM:SS".
        :param replace_microseconds: Whether to replace microseconds in the execution date with zeros.
        :return: A message indicating the status of the DAG run trigger.
        """
        endpoint = f"/api/experimental/dags/{dag_id}/dag_runs"
        url = urljoin(self._api_base_url, endpoint)
        data = {
            "run_id": run_id,
            "conf": conf,
            "execution_date": execution_date,
            "replace_microseconds": replace_microseconds,
        }
        return self._request(url, method="POST", json=data)["message"]

    def delete_dag(self, dag_id: str):
        """
        Delete a DAG.

        :param dag_id: The ID of the DAG to delete.
        :return: A message indicating the status of the DAG delete operation.
        """
        endpoint = f"/api/experimental/dags/{dag_id}/delete_dag"
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url, method="DELETE")
        return data["message"]

    def get_pool(self, name: str):
        """
        Get information about a specific pool.

        :param name: The name of the pool to retrieve information for.
        :return: A tuple containing the name of the pool, the number of
            slots in the pool, and a description of the pool.
        """
        endpoint = f"/api/experimental/pools/{name}"
        url = urljoin(self._api_base_url, endpoint)
        pool = self._request(url)
        return pool["pool"], pool["slots"], pool["description"]

    def get_pools(self):
        """
        Get a list of all pools.

        :return: A list of tuples, each containing the name of a pool,
            the number of slots in the pool, and a description of the pool.
        """
        endpoint = "/api/experimental/pools"
        url = urljoin(self._api_base_url, endpoint)
        pools = self._request(url)
        return [(p["pool"], p["slots"], p["description"]) for p in pools]

    def create_pool(self, name: str, slots: int, description: str, include_deferred: bool):
        """
        Create a new pool.

        :param name: The name of the pool to create.
        :param slots: The number of slots in the pool.
        :param description: A description of the pool.
        :param include_deferred: include deferred tasks in pool calculations

        :return: A tuple containing the name of the pool, the number of slots in the pool,
            a description of the pool and the include_deferred flag.
        """
        endpoint = "/api/experimental/pools"
        data = {
            "name": name,
            "slots": slots,
            "description": description,
            "include_deferred": include_deferred,
        }
        response = self._request(urljoin(self._api_base_url, endpoint), method="POST", json=data)
        return response["pool"], response["slots"], response["description"], response["include_deferred"]

    def delete_pool(self, name: str):
        """
        Delete a pool.

        :param name: The name of the pool to delete.
        :return: A tuple containing the name of the pool, the number
            of slots in the pool, and a description of the pool.
        """
        endpoint = f"/api/experimental/pools/{name}"
        url = urljoin(self._api_base_url, endpoint)
        pool = self._request(url, method="DELETE")
        return pool["pool"], pool["slots"], pool["description"]

    def get_lineage(self, dag_id: str, execution_date: str):
        """
        Get the lineage of a DAG run.

        :param dag_id: The ID of the DAG.
        :param execution_date: The execution date of the DAG run, in the format "YYYY-MM-DDTHH:MM:SS".
        :return: A message indicating the status of the lineage request.
        """
        endpoint = f"/api/experimental/lineage/{dag_id}/{execution_date}"
        url = urljoin(self._api_base_url, endpoint)
        data = self._request(url, method="GET")
        return data["message"]
