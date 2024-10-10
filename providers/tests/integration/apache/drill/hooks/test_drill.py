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
from __future__ import annotations

import os
import socket
from decimal import Decimal

import pytest

from airflow.providers.apache.drill.hooks.drill import DrillHook

AIRFLOW_CONN_DRILL_DEFAULT = "drill://{hostname}:8047"
TEST_TABLE_ID = "dfs.tmp.test_airflow"


@pytest.mark.integration("drill")
class TestDrillHook:
    def setup_method(self):
        os.environ["AIRFLOW_CONN_DRILL_DEFAULT"] = AIRFLOW_CONN_DRILL_DEFAULT.format(
            hostname=socket.gethostbyname("drill")
        )
        self.conn = DrillHook().get_conn()
        self.cursor = self.conn.cursor()
        sql = f"""
        create table {TEST_TABLE_ID} as
        select * from cp.`employee.json` limit 10
        """
        self.cursor.execute(sql)

    def teardown_method(self):
        self.cursor.execute(f"DROP TABLE {TEST_TABLE_ID}")
        self.conn.close()

    def test_execute(self):
        sql = f"select * from {TEST_TABLE_ID} limit 10"
        self.cursor.execute(sql)
        row = self.cursor.fetchone()
        assert row == (
            1,
            "Sheri Nowmer",
            "Sheri",
            "Nowmer",
            1,
            "President",
            0,
            1,
            "1961-08-26",
            "1994-12-01 00:00:00.0",
            Decimal("80000.0"),
            0,
            "Graduate Degree",
            "S",
            "F",
            "Senior Management",
            None,
        )
