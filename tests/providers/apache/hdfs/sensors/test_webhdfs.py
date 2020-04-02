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
import os
import unittest

from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from tests.providers.apache.hive import DEFAULT_DATE, TestHiveEnvironment


@unittest.skipIf(
    'AIRFLOW_RUNALL_TESTS' not in os.environ,
    "Skipped because AIRFLOW_RUNALL_TESTS is not set")
class TestWebHdfsSensor(TestHiveEnvironment):

    def test_webhdfs_sensor(self):
        op = WebHdfsSensor(
            task_id='webhdfs_sensor_check',
            filepath='hdfs://user/hive/warehouse/airflow.db/static_babynames',
            timeout=120,
            dag=self.dag)
        op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE,
               ignore_ti_state=True)
