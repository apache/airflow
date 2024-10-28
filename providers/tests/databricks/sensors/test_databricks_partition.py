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
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.databricks.sensors.databricks_partition import (
    DatabricksPartitionSensor,
)
from airflow.utils import timezone

TASK_ID = "db-partition-sensor"
DEFAULT_CONN_ID = "databricks_default"
HOST = "xx.cloud.databricks.com"
HOST_WITH_SCHEME = "https://xx.cloud.databricks.com"
PERSONAL_ACCESS_TOKEN = "token"

DEFAULT_SCHEMA = "schema1"
DEFAULT_CATALOG = "catalog1"
DEFAULT_TABLE = "table1"
DEFAULT_HTTP_PATH = "/sql/1.0/warehouses/xxxxx"
DEFAULT_SQL_WAREHOUSE = "sql_warehouse_default"
DEFAULT_CALLER = "TestDatabricksPartitionSensor"
DEFAULT_PARTITION = {"date": "2023-01-01"}
DEFAULT_DATE = timezone.datetime(2017, 1, 1)

TIMESTAMP_TEST = datetime.now() - timedelta(days=30)

sql_sensor = DatabricksPartitionSensor(
    databricks_conn_id=DEFAULT_CONN_ID,
    sql_warehouse_name=DEFAULT_SQL_WAREHOUSE,
    task_id=TASK_ID,
    table_name=DEFAULT_TABLE,
    schema=DEFAULT_SCHEMA,
    catalog=DEFAULT_CATALOG,
    partitions=DEFAULT_PARTITION,
    handler=fetch_all_handler,
)


class TestDatabricksPartitionSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)

        self.partition_sensor = DatabricksPartitionSensor(
            task_id=TASK_ID,
            databricks_conn_id=DEFAULT_CONN_ID,
            sql_warehouse_name=DEFAULT_SQL_WAREHOUSE,
            dag=self.dag,
            schema=DEFAULT_SCHEMA,
            catalog=DEFAULT_CATALOG,
            table_name=DEFAULT_TABLE,
            partitions={"date": "2023-01-01"},
            partition_operator="=",
            timeout=30,
            poke_interval=15,
            hook_params={"return_tuple": True},
        )

    def test_init(self):
        assert self.partition_sensor.databricks_conn_id == "databricks_default"
        assert self.partition_sensor.task_id == "db-partition-sensor"
        assert self.partition_sensor._sql_warehouse_name == "sql_warehouse_default"
        assert self.partition_sensor.poke_interval == 15

    @pytest.mark.parametrize(
        argnames=("sensor_poke_result", "expected_poke_result"),
        argvalues=[(True, True), (False, False)],
    )
    @patch.object(DatabricksPartitionSensor, "poke")
    def test_poke(self, mock_poke, sensor_poke_result, expected_poke_result):
        mock_poke.return_value = sensor_poke_result
        assert self.partition_sensor.poke({}) == expected_poke_result

    @pytest.mark.db_test
    def test_unsupported_conn_type(self):
        with pytest.raises(AirflowException):
            self.partition_sensor.run(
                start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
            )

    @patch.object(DatabricksPartitionSensor, "poke")
    def test_partition_sensor(self, patched_poke):
        patched_poke.return_value = True
        assert self.partition_sensor.poke({})

    @patch(
        "airflow.providers.databricks.sensors.databricks_partition.DatabricksPartitionSensor._sql_sensor"
    )
    def test_fail__generate_partition_query(self, _sql_sensor):
        table_name = "test"
        _sql_sensor.return_value = [[[], [], [], [], [], [], [], []]]
        with pytest.raises(
            AirflowException, match=f"Table {table_name} does not have partitions"
        ):
            self.partition_sensor._generate_partition_query(
                prefix="", suffix="", joiner_val="", table_name=table_name
            )

    @patch(
        "airflow.providers.databricks.sensors.databricks_partition.DatabricksPartitionSensor._sql_sensor"
    )
    def test_fail__generate_partition_query_with_partition_col_mismatch(
        self, _sql_sensor
    ):
        partition_col = "non_existent_col"
        partition_columns = ["col1", "col2"]
        _sql_sensor.return_value = [[[], [], [], [], [], [], [], partition_columns]]
        with pytest.raises(
            AirflowException, match=f"Column {partition_col} not part of table partitions"
        ):
            self.partition_sensor._generate_partition_query(
                prefix="",
                suffix="",
                joiner_val="",
                table_name="",
                opts={partition_col: "1"},
            )

    @patch(
        "airflow.providers.databricks.sensors.databricks_partition.DatabricksPartitionSensor._sql_sensor"
    )
    def test_fail__generate_partition_query_with_missing_opts(
        self,
        _sql_sensor,
    ):
        _sql_sensor.return_value = [[[], [], [], [], [], [], [], ["col1", "col2"]]]
        with pytest.raises(
            AirflowException, match="No partitions specified to check with the sensor."
        ):
            self.partition_sensor._generate_partition_query(
                prefix="", suffix="", joiner_val="", table_name=""
            )

    @patch(
        "airflow.providers.databricks.sensors.databricks_partition.DatabricksPartitionSensor"
        "._check_table_partitions"
    )
    def test_fail_poke(self, _check_table_partitions):
        partitions = "test"
        self.partition_sensor.partitions = partitions
        _check_table_partitions.return_value = False
        with pytest.raises(
            AirflowException,
            match=rf"Specified partition\(s\): {partitions} were not found.",
        ):
            self.partition_sensor.poke(context={})
