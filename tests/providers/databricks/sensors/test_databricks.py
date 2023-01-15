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

from airflow.providers.databricks.sensors.databricks import DatabricksSqlSensor

TASK_ID = "db-sensor"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
PARTITION_SENSOR = "table_partition"
DEFAULT_SQL_ENDPOINT = "sql_warehouse_default"
CHANGES_SENSOR = "table_changes"
PARTITIONS_SENSOR = "table_partition"

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)


class TestDatabricksSqlSensor:
    def setup_method(self):
        self.changes_sensor = DatabricksSqlSensor(
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_endpoint_name=DEFAULT_SQL_ENDPOINT,
            task_id=TASK_ID,
            table_name=DEFAULT_TABLE,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            partition_name={"part_col": "part_val"},
            db_sensor_type=CHANGES_SENSOR,
            timestamp=TIMESTAMP_TEST,
        )

        self.partition_sensor = DatabricksSqlSensor(
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_endpoint_name=DEFAULT_SQL_ENDPOINT,
            task_id=TASK_ID,
            table_name=DEFAULT_TABLE,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            partition_name={"part_col": "part_val"},
            db_sensor_type=PARTITION_SENSOR,
            timestamp=TIMESTAMP_TEST,
        )

    @mock.patch.object(DatabricksSqlSensor, "_check_table_changes", side_effect=(True,))
    def test_poke_changes_success(self, mock_poll_table_changes):
        assert self.changes_sensor.poke({}) is True

    @mock.patch.object(DatabricksSqlSensor, "_check_table_changes", side_effect=(False,))
    def test_poke_changes_failure(self, mock_poll_table_changes):
        assert self.changes_sensor.poke({}) is False

    @mock.patch.object(DatabricksSqlSensor, "_check_table_partitions", side_effect=(True,))
    def test_poke_partitions_success(self, mock_poll_table_changes):
        assert self.partition_sensor.poke({}) is True

    @mock.patch.object(DatabricksSqlSensor, "_check_table_partitions", side_effect=(False,))
    def test_poke_partitions_failure(self, mock_poll_table_changes):
        assert self.partition_sensor.poke({}) is False
