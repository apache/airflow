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

from unittest import mock

from airflow.providers.alibaba.cloud.sensors.analyticdb_spark import AnalyticDBSparkSensor
from airflow.utils import timezone

ADB_SPARK_SENSOR_STRING = "airflow.providers.alibaba.cloud.sensors.analyticdb_spark.{}"
DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MOCK_ADB_SPARK_CONN_ID = "mock_adb_spark_default"
MOCK_ADB_SPARK_ID = "mock_adb_spark_id"
MOCK_SENSOR_TASK_ID = "test-adb-spark-operator"
MOCK_REGION = "mock_region"


class TestAnalyticDBSparkSensor:
    def setup_method(self):
        self.sensor = AnalyticDBSparkSensor(
            app_id=MOCK_ADB_SPARK_ID,
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID,
            region=MOCK_REGION,
            task_id=MOCK_SENSOR_TASK_ID,
        )

    @mock.patch(ADB_SPARK_SENSOR_STRING.format("AnalyticDBSparkHook"))
    def test_get_hook(self, mock_service):
        """Test get_hook function works as expected."""
        self.sensor.hook
        mock_service.assert_called_once_with(
            adb_spark_conn_id=MOCK_ADB_SPARK_CONN_ID, region=MOCK_REGION
        )

    @mock.patch(ADB_SPARK_SENSOR_STRING.format("AnalyticDBSparkSensor.hook"))
    def test_poke_terminal_state(self, mock_service):
        """Test poke_terminal_state works as expected with COMPLETED application."""
        # Given
        mock_service.get_spark_state.return_value = "COMPLETED"

        # When
        res = self.sensor.poke(None)

        # Then
        assert res is True
        mock_service.get_spark_state.assert_called_once_with(MOCK_ADB_SPARK_ID)

    @mock.patch(ADB_SPARK_SENSOR_STRING.format("AnalyticDBSparkSensor.hook"))
    def test_poke_non_terminal_state(self, mock_service):
        """Test poke_terminal_state works as expected with RUNNING application."""
        # Given
        mock_service.get_spark_state.return_value = "RUNNING"

        # When
        res = self.sensor.poke(None)

        # Then
        assert res is False
        mock_service.get_spark_state.assert_called_once_with(MOCK_ADB_SPARK_ID)
