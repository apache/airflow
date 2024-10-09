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
from datetime import timedelta
from unittest import mock

import pytest

from airflow.exceptions import AirflowSensorTimeout
from airflow.models.dag import DAG
from airflow.providers.apache.hive.sensors.named_hive_partition import NamedHivePartitionSensor
from airflow.utils.timezone import datetime

from providers.tests.apache.hive import MockHiveMetastoreHook, TestHiveEnvironment

DEFAULT_DATE = datetime(2015, 1, 1)
DEFAULT_DATE_ISO = DEFAULT_DATE.isoformat()
DEFAULT_DATE_DS = DEFAULT_DATE_ISO[:10]

pytestmark = pytest.mark.db_test


class TestNamedHivePartitionSensor:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", schedule=None, default_args=args)
        self.next_day = (DEFAULT_DATE + timedelta(days=1)).isoformat()[:10]
        self.database = "airflow"
        self.partition_by = "ds"
        self.table = "static_babynames_partitioned"
        self.hql = """
                CREATE DATABASE IF NOT EXISTS {{ params.database }};
                USE {{ params.database }};
                DROP TABLE IF EXISTS {{ params.table }};
                CREATE TABLE IF NOT EXISTS {{ params.table }} (
                    state string,
                    year string,
                    name string,
                    gender string,
                    num int)
                PARTITIONED BY ({{ params.partition_by }} string);
                ALTER TABLE {{ params.table }}
                ADD PARTITION({{ params.partition_by }}='{{ ds }}');
                """
        self.hook = MockHiveMetastoreHook()

    def test_parse_partition_name_correct(self):
        schema = "default"
        table = "users"
        partition = "ds=2016-01-01/state=IT"
        name = f"{schema}.{table}/{partition}"
        parsed_schema, parsed_table, parsed_partition = NamedHivePartitionSensor.parse_partition_name(name)
        assert schema == parsed_schema
        assert table == parsed_table
        assert partition == parsed_partition

    def test_parse_partition_name_incorrect(self):
        name = "incorrect.name"
        with pytest.raises(ValueError):
            NamedHivePartitionSensor.parse_partition_name(name)

    def test_parse_partition_name_default(self):
        table = "users"
        partition = "ds=2016-01-01/state=IT"
        name = f"{table}/{partition}"
        parsed_schema, parsed_table, parsed_partition = NamedHivePartitionSensor.parse_partition_name(name)
        assert "default" == parsed_schema
        assert table == parsed_table
        assert partition == parsed_partition

    def test_poke_existing(self):
        self.hook.metastore.__enter__().check_for_named_partition.return_value = True
        partitions = [f"{self.database}.{self.table}/{self.partition_by}={DEFAULT_DATE_DS}"]
        sensor = NamedHivePartitionSensor(
            partition_names=partitions,
            task_id="test_poke_existing",
            poke_interval=1,
            hook=self.hook,
            dag=self.dag,
        )
        assert sensor.poke(None)
        self.hook.metastore.__enter__().check_for_named_partition.assert_called_with(
            self.database, self.table, f"{self.partition_by}={DEFAULT_DATE_DS}"
        )

    def test_poke_non_existing(self):
        self.hook.metastore.__enter__().check_for_named_partition.return_value = False
        partitions = [f"{self.database}.{self.table}/{self.partition_by}={self.next_day}"]
        sensor = NamedHivePartitionSensor(
            partition_names=partitions,
            task_id="test_poke_non_existing",
            poke_interval=1,
            hook=self.hook,
            dag=self.dag,
        )
        assert not sensor.poke(None)
        self.hook.metastore.__enter__().check_for_named_partition.assert_called_with(
            self.database, self.table, f"{self.partition_by}={self.next_day}"
        )


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
class TestPartitions(TestHiveEnvironment):
    def test_succeeds_on_one_partition(self):
        mock_hive_metastore_hook = MockHiveMetastoreHook()
        mock_hive_metastore_hook.check_for_named_partition = mock.MagicMock(return_value=True)

        op = NamedHivePartitionSensor(
            task_id="hive_partition_check",
            partition_names=["airflow.static_babynames_partitioned/ds={{ds}}"],
            dag=self.dag,
            hook=mock_hive_metastore_hook,
        )

        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        mock_hive_metastore_hook.check_for_named_partition.assert_called_once_with(
            "airflow", "static_babynames_partitioned", "ds=2015-01-01"
        )

    def test_succeeds_on_multiple_partitions(self):
        mock_hive_metastore_hook = MockHiveMetastoreHook()
        mock_hive_metastore_hook.check_for_named_partition = mock.MagicMock(return_value=True)

        op = NamedHivePartitionSensor(
            task_id="hive_partition_check",
            partition_names=[
                "airflow.static_babynames_partitioned/ds={{ds}}",
                "airflow.static_babynames_partitioned2/ds={{ds}}",
            ],
            dag=self.dag,
            hook=mock_hive_metastore_hook,
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
        mock_hive_metastore_hook.check_for_named_partition.assert_any_call(
            "airflow", "static_babynames_partitioned", "ds=2015-01-01"
        )
        mock_hive_metastore_hook.check_for_named_partition.assert_any_call(
            "airflow", "static_babynames_partitioned2", "ds=2015-01-01"
        )

    def test_parses_partitions_with_periods(self):
        name = NamedHivePartitionSensor.parse_partition_name(
            partition="schema.table/part1=this.can.be.an.issue/part2=ok"
        )
        assert name[0] == "schema"
        assert name[1] == "table"
        assert name[2] == "part1=this.can.be.an.issue/part2=ok"

    def test_times_out_on_nonexistent_partition(self):
        mock_hive_metastore_hook = MockHiveMetastoreHook()
        mock_hive_metastore_hook.check_for_named_partition = mock.MagicMock(return_value=False)

        op = NamedHivePartitionSensor(
            task_id="hive_partition_check",
            partition_names=[
                "airflow.static_babynames_partitioned/ds={{ds}}",
                "airflow.static_babynames_partitioned/ds=nonexistent",
            ],
            poke_interval=0.1,
            timeout=1,
            dag=self.dag,
            hook=mock_hive_metastore_hook,
        )
        with pytest.raises(AirflowSensorTimeout):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
