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
from json import JSONDecodeError
from os.path import dirname, join
from typing import TYPE_CHECKING, Any, Iterable, TypeVar
from unittest.mock import MagicMock

import pytest
from httpx import Headers, Response
from msgraph_core import APIVersion

from airflow.models import Connection
from airflow.utils.context import Context

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

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


def mock_connection(schema: str | None = None, host: str | None = None) -> Connection:
    connection = MagicMock(spec=Connection)
    connection.schema = schema
    connection.host = host
    return connection


def mock_json_response(status_code, *contents) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    response.headers = Headers({})
    response.content = b""
    if contents:
        response.json.side_effect = list(contents)
    else:
        response.json.return_value = None
    return response


def mock_response(status_code, content: Any = None, headers: dict | None = None) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    response.headers = Headers(headers or {})
    response.content = content
    response.json.side_effect = JSONDecodeError("", "", 0)
    return response


def mock_context(task) -> Context:
    from datetime import datetime

    from airflow.models import TaskInstance
    from airflow.utils.session import NEW_SESSION
    from airflow.utils.state import TaskInstanceState
    from airflow.utils.xcom import XCOM_RETURN_KEY

    values: dict[str, Any] = {}

    class MockedTaskInstance(TaskInstance):
        def __init__(
            self,
            task,
            execution_date: datetime | None = None,
            run_id: str | None = "run_id",
            state: str | None = TaskInstanceState.RUNNING,
            map_index: int = -1,
        ):
            super().__init__(
                task=task, execution_date=execution_date, run_id=run_id, state=state, map_index=map_index
            )
            self.values: dict[str, Any] = {}

        def xcom_pull(
            self,
            task_ids: Iterable[str] | str | None = None,
            dag_id: str | None = None,
            key: str = XCOM_RETURN_KEY,
            include_prior_dates: bool = False,
            session: Session = NEW_SESSION,
            *,
            map_indexes: Iterable[int] | int | None = None,
            default: Any | None = None,
        ) -> Any:
            return values.get(f"{task_ids or self.task_id}_{dag_id or self.dag_id}_{key}")

        def xcom_push(
            self,
            key: str,
            value: Any,
            execution_date: datetime | None = None,
            session: Session = NEW_SESSION,
        ) -> None:
            values[f"{self.task_id}_{self.dag_id}_{key}"] = value

    values["ti"] = MockedTaskInstance(task=task)

    # See https://github.com/python/mypy/issues/8890 - mypy does not support passing typed dict to TypedDict
    return Context(values)  # type: ignore[misc]


def load_json(*args: str):
    with open(join(dirname(__file__), "azure", join(*args)), encoding="utf-8") as file:
        return json.load(file)


def load_file(*args: str, mode="r", encoding="utf-8"):
    with open(join(dirname(__file__), "azure", join(*args)), mode=mode, encoding=encoding) as file:
        return file.read()


def get_airflow_connection(
    conn_id: str,
    login: str = "client_id",
    password: str = "client_secret",
    tenant_id: str = "tenant-id",
    proxies: dict | None = None,
    api_version: APIVersion = APIVersion.v1,
):
    from airflow.models import Connection

    return Connection(
        schema="https",
        conn_id=conn_id,
        conn_type="http",
        host="graph.microsoft.com",
        port=80,
        login=login,
        password=password,
        extra={"tenant_id": tenant_id, "api_version": api_version.value, "proxies": proxies or {}},
    )
