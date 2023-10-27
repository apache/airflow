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

import pytest
from moto import mock_sts
from moto.core import DEFAULT_ACCOUNT_ID

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor

DATA_SET_ID = "DemoDataSet"
INGESTION_ID = "DemoDataSet_Ingestion"


class TestQuickSightSensor:
    def setup_method(self):
        self.sensor = QuickSightSensor(
            task_id="test_quicksight_sensor",
            aws_conn_id="aws_default",
            data_set_id="DemoDataSet",
            ingestion_id="DemoDataSet_Ingestion",
        )

    @mock_sts
    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_success(self, mock_get_status):
        mock_get_status.return_value = "COMPLETED"
        assert self.sensor.poke({}) is True
        mock_get_status.assert_called_once_with(DEFAULT_ACCOUNT_ID, DATA_SET_ID, INGESTION_ID)

    @mock_sts
    @mock.patch.object(QuickSightHook, "get_status")
    @mock.patch.object(QuickSightHook, "get_error_info")
    def test_poke_cancelled(self, _, mock_get_status):
        mock_get_status.return_value = "CANCELLED"
        with pytest.raises(AirflowException):
            self.sensor.poke({})
        mock_get_status.assert_called_once_with(DEFAULT_ACCOUNT_ID, DATA_SET_ID, INGESTION_ID)

    @mock_sts
    @mock.patch.object(QuickSightHook, "get_status")
    @mock.patch.object(QuickSightHook, "get_error_info")
    def test_poke_failed(self, _, mock_get_status):
        mock_get_status.return_value = "FAILED"
        with pytest.raises(AirflowException):
            self.sensor.poke({})
        mock_get_status.assert_called_once_with(DEFAULT_ACCOUNT_ID, DATA_SET_ID, INGESTION_ID)

    @mock_sts
    @mock.patch.object(QuickSightHook, "get_status")
    def test_poke_initialized(self, mock_get_status):
        mock_get_status.return_value = "INITIALIZED"
        assert self.sensor.poke({}) is False
        mock_get_status.assert_called_once_with(DEFAULT_ACCOUNT_ID, DATA_SET_ID, INGESTION_ID)

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, AirflowException), (True, AirflowSkipException))
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.sts.StsHook.get_account_number")
    @mock.patch("airflow.providers.amazon.aws.hooks.quicksight.QuickSightHook.get_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.quicksight.QuickSightHook.get_error_info")
    def test_fail_poke(self, get_error_info, get_status, _, soft_fail, expected_exception):
        self.sensor.soft_fail = soft_fail
        error = "expected error"
        message = f"The QuickSight Ingestion failed. Error info: {error}"
        with pytest.raises(expected_exception, match=message):
            get_status.return_value = "FAILED"
            get_error_info.return_value = message
            self.sensor.poke(context={})
