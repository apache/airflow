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
from tests.providers.apache.hive import DEFAULT_DATE, DEFAULT_DATE_DS, MockDBConnection, TestHiveEnvironment


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
class TestHivePartitionSensor(TestHiveEnvironment):
    def test_hive_metastore_sql_sensor(self):
        op = MetastorePartitionSensor(
            task_id="hive_partition_check",
            conn_id="test_connection_id",
            sql="test_sql",
            table="airflow.static_babynames_partitioned",
            partition_name=f"ds={DEFAULT_DATE_DS}",
            dag=self.dag,
        )
        op._get_hook = mock.MagicMock(return_value=MockDBConnection({}))
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
