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
from __future__ import annotations

from typing import Any

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from urllib3.util.retry import Retry


def generate_access_token(username: str, password: str, host: str) -> str:
    """
    Generate valid access token for the given username and password.

    Note: API server is currently using Simple Auth Manager.

    :param username: The username to use for the login
    :param password: The password to use for the login
    :param host: The host to use for the login
    :return: The access token
    """
    Retry.DEFAULT_BACKOFF_MAX = 32
    # retry for rate limit errors (429) and server errors (500, 502, 503, 504)
    retry = Retry(total=10, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    # Backoff Retry Formula: min(1 × (2^(retry - 1)), 32) seconds
    # 1 + 2 + 4 + 8 + 16 + 32 + 32 + 32 + 32 + 32 = 191 sec (~3.2 min)
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))

    api_server_url = host
    if not api_server_url.startswith(("http://", "https://")):
        api_server_url = "http://" + host
    url = f"{api_server_url}/auth/token"

    login_response = session.post(
        url,
        json={"username": username, "password": password},
    )
    access_token = login_response.json().get("access_token")

    assert access_token, f"Failed to get JWT token from redirect url {url} with status code {login_response}"
    return access_token


def make_authenticated_rest_api_request(
    path: str,
    method: str,
    body: dict | None = None,
    username: str = "admin",
    password: str = "admin",
):
    from airflow.configuration import conf

    api_server_url = conf.get("api", "base_url", fallback="http://localhost:8080").rstrip("/")
    skip_auth = conf.getboolean("core", "simple_auth_manager_all_admins", fallback=False)
    headers = {}
    if not skip_auth:
        token = generate_access_token(username, password, api_server_url)
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(
        method=method,
        url=api_server_url + path,
        headers=headers,
        json=body,
    )
    response.raise_for_status()
    if response.text != "":
        return response.json()


def create_connection_request(connection_id: str, connection: dict[str, Any]):
    return make_authenticated_rest_api_request(
        path="/api/v2/connections",
        method="POST",
        body={
            "connection_id": connection_id,
            **connection,
        },
    )


def delete_connection_request(connection_id: str):
    return make_authenticated_rest_api_request(
        path=f"/api/v2/connections/{connection_id}",
        method="DELETE",
    )


def create_airflow_connection(connection_id: str, connection_conf: dict[str, Any]) -> None:
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    print(f"Removing connection '{connection_id}' if it exists")
    if AIRFLOW_V_3_0_PLUS:
        try:
            delete_connection_request(connection_id=connection_id)
        except HTTPError:
            print(f"Connection '{connection_id}' does not exist. A new one will be created")
        create_connection_request(connection_id=connection_id, connection=connection_conf)
    else:
        from airflow.models import Connection
        from airflow.settings import Session

        if Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = Session()
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        connection = Connection(conn_id=connection_id, **connection_conf)
        session.add(connection)
        session.commit()
    print(f"Connection '{connection_id}' created")


def delete_airflow_connection(connection_id: str) -> None:
    from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

    print(f"Removing connection '{connection_id}'")
    if AIRFLOW_V_3_0_PLUS:
        delete_connection_request(connection_id=connection_id)
    else:
        from airflow.models import Connection
        from airflow.settings import Session

        if Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = Session()
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        session.commit()
