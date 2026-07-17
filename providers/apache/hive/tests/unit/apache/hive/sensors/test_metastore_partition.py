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
from unittest import mock

import pytest

from airflow.providers.apache.hive.sensors.metastore_partition import MetastorePartitionSensor

from unit.apache.hive import DEFAULT_DATE, DEFAULT_DATE_DS, MockDBConnection, TestHiveEnvironment


@pytest.mark.parametrize(
    ("table", "schema", "expected_parameters"),
    [
        (
            "airflow.static_babynames_partitioned",
            "default",
            {
                "table": "static_babynames_partitioned",
                "schema": "airflow",
                "partition_name": f"ds={DEFAULT_DATE_DS}",
            },
        ),
        (
            "static_babynames_partitioned",
            "airflow",
            {
                "table": "static_babynames_partitioned",
                "schema": "airflow",
                "partition_name": f"ds={DEFAULT_DATE_DS}",
            },
        ),
    ],
)
def test_metastore_partition_sensor_uses_documented_constructor_and_parameters(
    table, schema, expected_parameters
):
    op = MetastorePartitionSensor(
        task_id="hive_partition_check",
        mysql_conn_id="test_connection_id",
        table=table,
        schema=schema,
        partition_name=f"ds={DEFAULT_DATE_DS}",
    )
    hook = mock.MagicMock()
    hook.get_records.return_value = [["test_record"]]
    op._get_hook = mock.MagicMock(return_value=hook)

    assert op.conn_id == "test_connection_id"
    assert op.poke({}) is True

    hook.get_records.assert_called_once()
    sql, parameters = hook.get_records.call_args.args
    assert "B0.TBL_NAME = %(table)s" in sql
    assert "C0.NAME = %(schema)s" in sql
    assert "A0.PART_NAME = %(partition_name)s" in sql
    assert parameters == expected_parameters


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
class TestHivePartitionSensor(TestHiveEnvironment):
    def test_hive_metastore_sql_sensor(self):
        op = MetastorePartitionSensor(
            task_id="hive_partition_check",
            mysql_conn_id="test_connection_id",
            table="airflow.static_babynames_partitioned",
            partition_name=f"ds={DEFAULT_DATE_DS}",
            dag=self.dag,
        )
        op._get_hook = mock.MagicMock(return_value=MockDBConnection({}))
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
