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
from unittest.mock import patch

import pytest

from airflow.providers.apache.hive.sensors.hive_partition import HivePartitionSensor
from tests.providers.apache.hive import DEFAULT_DATE, MockHiveMetastoreHook, TestHiveEnvironment


@pytest.mark.skipif(
    "AIRFLOW_RUNALL_TESTS" not in os.environ, reason="Skipped because AIRFLOW_RUNALL_TESTS is not set"
)
@patch(
    "airflow.providers.apache.hive.sensors.hive_partition.HiveMetastoreHook",
    side_effect=MockHiveMetastoreHook,
)
class TestHivePartitionSensor(TestHiveEnvironment):
    def test_hive_partition_sensor(self, mock_hive_metastore_hook):
        op = HivePartitionSensor(
            task_id="hive_partition_check", table="airflow.static_babynames_partitioned", dag=self.dag
        )
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)
