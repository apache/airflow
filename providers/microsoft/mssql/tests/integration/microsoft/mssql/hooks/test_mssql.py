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

import os

import pytest
from pymssql import Connection

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

AIRFLOW_CONN_MSSQL_DEFAULT = "mssql://sa:airflow123@mssql:1433/"


@pytest.mark.integration("mssql")
class TestMsSqlHook:
    def setup_method(self):
        os.environ["AIRFLOW_CONN_MSSQL_DEFAULT"] = AIRFLOW_CONN_MSSQL_DEFAULT
        self.hook = MsSqlHook()

    def test_get_conn(self):
        assert isinstance(self.hook.get_conn(), Connection)

    def test_autocommit(self):
        conn = self.hook.get_conn()
        self.hook.set_autocommit(conn=conn, autocommit=True)
        assert self.hook.get_autocommit(conn=conn)
        self.hook.set_autocommit(conn=conn, autocommit=False)
        assert not self.hook.get_autocommit(conn=conn)
