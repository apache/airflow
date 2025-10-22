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
    """create a mocked Airflwo connection for Impala."""
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
    return ImpalaHook(imapala_conn_id=DEFAULT_CONN_ID)