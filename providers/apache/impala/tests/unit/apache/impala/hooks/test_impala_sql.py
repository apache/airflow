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

from typing import TYPE_CHECKING
from apache.impala.src.airflow.providers.apache.impala.hooks.impala import ImpalaHook

from impala.dbapi import connect
import json
import pytest
from sqlalchemy.engine.url import make_url
from unittest.mock import patch, MagicMock
from airflow.models import Connection
from sqlalchemy.engine import URL

from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from impala.interface import Connection
    
    

DEFAULT_CONN_ID = "impala_default"
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 21050
DEFAULT_LOGIN = "user"
DEFAULT_PASSWORD = "pass"
DEFAULT_SCHEMA = "default_db"


@pytest.fixture
def mock_connection(create_connection_without_db)-> Connection:
    """create a mocked Airflow connection for Impala."""
    conn = Connection(
        conn_id=DEFAULT_CONN_ID,
        conn_type="impala",
        host=DEFAULT_HOST,
        login=DEFAULT_LOGIN,
        password=DEFAULT_PASSWORD,
        port=DEFAULT_PORT,
        schema=DEFAULT_SCHEMA,
    )
    create_connection_without_db(conn)
    return conn

@pytest.fixture
def impala_hook()-> ImpalaHook:
    """Fixture for ImpalaHook with mocked connection"""
    return ImpalaHook(impala_conn_id=DEFAULT_CONN_ID)

def get_cursor_descriptions(fields: list[str]) -> list[tuple[str]]:
    return [(field,) for field in fields]

@pytest.mark.parametrize(
    "host, login, password, port, schema, extra_dict, expected_query",
    [
        # Basic Impala connection, no extras
        (
            "localhost",
            "user",
            "pass",
            21050,
            "default_db",
            {},
            {},
        ),
        # SSL enabled
        (
            "impala-secure.company.com",
            "secure_user",
            "secret",
            21050,
            "analytics",
            {"use_ssl": True},
            {"use_ssl": True},
        ),
        # Kerberos authentication
        (
            "impala-kerberos.company.com",
            "kerb_user",
            None,
            21050,
            "sales",
            {"auth_mechanism": "GSSAPI", "kerberos_service_name": "impala"},
            {"auth_mechanism": "GSSAPI", "kerberos_service_name": "impala"},
        ),
        # Connection with timeout
        (
            "impala.company.com",
            "timeout_user",
            "pw123",
            21050,
            "warehouse",
            {"timeout": 30},
            {"timeout": 30},
        ),
    ],
)

def test_sqlalchemy_url_property(
    impala_hook,
    mock_connection,
    host,
    login,
    password,
    port,
    schema,
    extra_dict,
    expected_query
):
    """Tests various custom configurations passed via the 'extra' field."""
    mock_connection.host = host
    mock_connection.login = login
    mock_connection.password = password
    mock_connection.port = port
    mock_connection.schema = schema
    mock_connection.extra = json.dumps(extra_dict) if extra_dict else None
    
    with patch.object(impala_hook, "get_connection", return_value=mock_connection):
        url = impala_hook.sqlalchemy_url
        
    assert url.drivername == "impala"
    assert url.username == login
    assert url.password == password or ""
    assert url.host == host
    assert url.port == port
    assert url.database == schema
    assert url.query == expected_query
    
        
@pytest.mark.parametrize(
    "sql, expected_rows",
    [
        ("SELECT * FROM users", [("Alice", 1), ("Bob", 2)]),
        ("SELECT 1", [(1,)]),
    ]
)

def test_impala_run_query(impala_hook, mock_connection, sql, expected_rows):
    cursor = MagicMock()
    cursor.fetchall.return_value = expected_rows
    cursor.description = get_cursor_descriptions([f"col{i}" for i in range(len(expected_rows[0]))])

    mock_conn = MagicMock()
    mock_conn.host = mock_connection.host
    mock_conn.login = mock_connection.login
    mock_conn.password = mock_connection.password
    mock_conn.schema = mock_connection.schema
    mock_conn.cursor.return_value = cursor

    with patch.object(impala_hook, "get_connection", return_value=mock_conn):
        result = impala_hook.run(sql, handler=lambda cur: cur.fetchall())

    cursor.execute.assert_called_once_with(sql)
    assert result == expected_rows
    
    
def test_get_sqlalchemy_engine(impala_hook, mock_connection, mocker):
    mock_create_engine = mocker.patch("airflow.providers.common.sql.create_engine", autospec=True)
    mock_engine=MagicMock()
    mock_create_engine.return_value = mock_engine
    
    with patch.object(impala_hook, "get_connection", return_value=mock_connection):
        engine = impala_hook.get_sqlalchemy_engine()
        
    assert engine is mock_engine
    
    call_args = mock_create_engine.call_args[1]
    actual_url = call_args["url"]
    
    assert actual_url.drivername == "impala"
    assert actual_url.host == DEFAULT_HOST
    assert actual_url.username == DEFAULT_LOGIN
    assert actual_url.password == DEFAULT_PASSWORD
    assert actual_url.port == DEFAULT_PORT
    assert actual_url.database == DEFAULT_SCHEMA
    assert actual_url.query == {}
    
    