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

import pytest
from adbc_driver_manager import dbapi

from airflow.models import Connection
from airflow.providers.apache.arrow.hooks.adbc import AdbcHook


class TestAdbcHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="adbc_default",
                conn_type="adbc",
                host="file::memory:?cache=shared",
                extra=json.dumps(
                    {
                        "driver": "adbc_driver_sqlite",
                    }
                ),
            )
        )

    def test_dbapi_connection(self):
        adbc_conn = AdbcHook().get_conn()
        assert isinstance(adbc_conn, dbapi.Connection)
