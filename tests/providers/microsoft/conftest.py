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

import json
import random
import string
from os.path import join, dirname
from typing import TypeVar, Iterable, Any, Optional, Dict
from unittest.mock import MagicMock

import pytest
from httpx import Response
from msgraph_core import APIVersion

from airflow.models import Connection

T = TypeVar("T", dict, str, Connection)


@pytest.fixture
def create_mock_connection(monkeypatch):
    """Helper fixture for create test connection."""

    def wrapper(conn: T, conn_id: str | None = None):
        conn_id = conn_id or "test_conn_" + "".join(
            random.choices(string.ascii_lowercase + string.digits, k=6)
        )
        if isinstance(conn, dict):
            conn = Connection.from_json(conn)
        elif isinstance(conn, str):
            conn = Connection(uri=conn)

        if not isinstance(conn, Connection):
            raise TypeError(
                f"Fixture expected either JSON, URI or Connection type, but got {type(conn).__name__}"
            )
        if not conn.conn_id:
            conn.conn_id = conn_id

        monkeypatch.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", conn.get_uri())
        return conn

    return wrapper


@pytest.fixture
def create_mock_connections(create_mock_connection):
    """Helper fixture for create multiple test connections."""

    def wrapper(*conns: T):
        return list(map(create_mock_connection, conns))

    return wrapper


@pytest.fixture
def mocked_connection(request, create_mock_connection):
    """Helper indirect fixture for create test connection."""
    return create_mock_connection(request.param)


def mock_connection(schema: Optional[str] = None, host: Optional[str] = None) -> Connection:
    connection = MagicMock(spec=Connection)
    connection.schema = schema
    connection.host = host
    return connection


def mock_json_response(status_code, *contents) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    if contents:
        contents = list(contents)
        response.json.side_effect = lambda: contents.pop(0)
    else:
        response.json.return_value = None
    return response


def mock_response(status_code, content: Any = None) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    response.content = content
    return response


def load_json(*locations: Iterable[str]):
    with open(join(dirname(__file__), "azure", join(*locations)), encoding="utf-8") as file:
        return json.load(file)


def load_file(*locations: Iterable[str], mode="r", encoding="utf-8"):
    with open(join(dirname(__file__), "azure", join(*locations)), mode=mode, encoding=encoding) as file:
        return file.read()


def get_airflow_connection(
        conn_id: str,
        login: str = "client_id",
        password: str = "client_secret",
        tenant_id: str = "tenant-id",
        proxies: Optional[Dict] = None,
        api_version: APIVersion = APIVersion.v1):
    from airflow.models import Connection

    return Connection(
        schema="https",
        conn_id=conn_id,
        conn_type="http",
        host="graph.microsoft.com",
        port="80",
        login=login,
        password=password,
        extra={"tenant_id": tenant_id, "api_version": api_version.value, "proxies": proxies or {}},
    )
