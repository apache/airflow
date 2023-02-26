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
#
from __future__ import annotations

from datetime import datetime, timedelta
from unittest import mock

from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.sensors.table_changes import DatabricksTableChangesSensor

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
DEFAULT_SQL_ENDPOINT = "sql_warehouse_default"

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)

sql_sensor = DatabricksTableChangesSensor(
    databricks_conn_id=DEFAULT_CONN_ID,
    sql_endpoint_name=DEFAULT_SQL_ENDPOINT,
    task_id=TASK_ID,
    table_name=DEFAULT_TABLE,
    schema=DEFAULT_SCHEMA,
    catalog=DEFAULT_CATALOG,
    handler=fetch_all_handler,
)


class TestDatabricksTableChangesSensor:
    @mock.patch.object(DatabricksTableChangesSensor, "_get_results_table_changes", return_value=True)
    @mock.patch.object(DatabricksTableChangesSensor, "get_current_table_version", return_value=1)
    @mock.patch.object(DatabricksTableChangesSensor, "get_previous_version", return_value=1)
    def test_poke_changes_success_same_version(
        self, mock_check_table_changes, mock_get_current_table_version, mock_get_previous_version
    ):
        assert sql_sensor.poke({}) == 1

    @mock.patch.object(DatabricksTableChangesSensor, "_get_results_table_changes", return_value=False)
    @mock.patch.object(DatabricksTableChangesSensor, "get_current_table_version", return_value=1)
    @mock.patch.object(DatabricksTableChangesSensor, "get_previous_version", return_value=1)
    def test_poke_changes_failure(
        self, mock_check_table_changes, mock_get_current_table_version, mock_get_previous_version
    ):
        assert sql_sensor.poke({}) != 1

    @mock.patch.object(DatabricksTableChangesSensor, "_get_results_table_changes", return_value=True)
    @mock.patch.object(DatabricksTableChangesSensor, "get_current_table_version", return_value=2)
    @mock.patch.object(DatabricksTableChangesSensor, "get_previous_version", return_value=1)
    def test_poke_changes_success_new_version(
        self, mock_check_table_changes, mock_get_current_table_version, mock_get_previous_version
    ):
        assert sql_sensor.poke({}) == 1

    @mock.patch.object(DatabricksTableChangesSensor, "_get_results_table_changes", return_value=True)
    @mock.patch.object(DatabricksTableChangesSensor, "get_current_table_version", return_value=1)
    @mock.patch.object(DatabricksTableChangesSensor, "get_previous_version", return_value=None)
    def test_poke_changes_success_first_version(
        self, mock_check_table_changes, mock_get_current_table_version, mock_get_previous_version
    ):
        assert sql_sensor.poke({}) == 1
