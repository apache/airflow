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

import datetime
import unittest
from unittest import mock

from airflow.models.dag import DAG
from airflow.providers.apache.hive.transfers.vertica_to_hive import VerticaToHiveOperator


def mock_get_conn():
    commit_mock = mock.MagicMock()
    cursor_mock = mock.MagicMock(
        execute=[],
        fetchall=[["1", "2", "3"]],
        description=["a", "b", "c"],
        iterate=[["1", "2", "3"]],
    )
    conn_mock = mock.MagicMock(
        commit=commit_mock,
        cursor=cursor_mock,
    )
    return conn_mock


class TestVerticaToHiveTransfer(unittest.TestCase):
    def setUp(self):
        args = {"owner": "airflow", "start_date": datetime.datetime(2017, 1, 1)}
        self.dag = DAG("test_dag_id", default_args=args)

    @mock.patch(
        "airflow.providers.apache.hive.transfers.vertica_to_hive.VerticaHook.get_conn",
        side_effect=mock_get_conn,
    )
    @mock.patch("airflow.providers.apache.hive.transfers.vertica_to_hive.HiveCliHook.load_file")
    def test_select_insert_transfer(self, *args):
        """
        Test check selection from vertica into memory and
        after that inserting into mysql
        """
        task = VerticaToHiveOperator(
            task_id="test_task_id",
            sql="select a, b, c",
            hive_table="test_table",
            vertica_conn_id="test_vertica_conn_id",
            hive_cli_conn_id="hive_cli_default",
            dag=self.dag,
        )
        task.execute(None)
