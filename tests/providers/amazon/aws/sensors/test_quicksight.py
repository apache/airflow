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

import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor


class TestQuickSightSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = QuickSightSensor(
            task_id="test_dms_sensor",
            aws_conn_id="aws_default",
            aws_account_id="123456789012",
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
        )

    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_success(self, mock_get_status):
        mock_get_status.return_value = "COMPLETED"
        self.assertTrue(self.sensor.poke({}))
        mock_get_status.assert_called_once_with("123456789012", "DemoDataSet", "DemoDataSet_Ingestion")

    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_cancelled(self, mock_get_status):

        mock_get_status.return_value = "CANCELLED"
        with self.assertRaises(AirflowException):
            self.sensor.poke({})
        mock_get_status.assert_called_once_with("123456789012", "DemoDataSet", "DemoDataSet_Ingestion")

    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_failed(self, mock_get_status):
        mock_get_status.return_value = "FAILED"
        with self.assertRaises(AirflowException):
            self.sensor.poke({})
        mock_get_status.assert_called_once_with("123456789012", "DemoDataSet", "DemoDataSet_Ingestion")

    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_initialized(self, mock_get_status):
        mock_get_status.return_value = "INITIALIZED"
        self.assertFalse(self.sensor.poke({}))
        mock_get_status.assert_called_once_with("123456789012", "DemoDataSet", "DemoDataSet_Ingestion")
