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

import pathlib
import random
import string
from typing import TypeVar

import pytest

from airflow.models import Connection
from airflow.providers.microsoft.azure.hooks.powerbi import PowerBIHook

pytest_plugins = "tests_common.pytest_plugin"

T = TypeVar("T", dict, str, Connection)


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    deprecations_ignore_path = pathlib.Path(__file__).parent.joinpath("deprecations_ignore.yml")
    dep_path = [deprecations_ignore_path] if deprecations_ignore_path.exists() else []
    config.inicfg["airflow_deprecations_ignore"] = (
        config.inicfg.get("airflow_deprecations_ignore", []) + dep_path  # type: ignore[assignment,operator]
    )


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


@pytest.fixture
def powerbi_hook():
    return PowerBIHook(**{"conn_id": "powerbi_conn_id", "timeout": 3, "api_version": "v1.0"})
