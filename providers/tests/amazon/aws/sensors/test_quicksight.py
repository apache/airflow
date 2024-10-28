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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.quicksight import QuickSightHook
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor

DATA_SET_ID = "DemoDataSet"
INGESTION_ID = "DemoDataSet_Ingestion"


@pytest.fixture
def mocked_get_status():
    with mock.patch.object(QuickSightHook, "get_status") as m:
        yield m


@pytest.fixture
def mocked_get_error_info():
    with mock.patch.object(QuickSightHook, "get_error_info") as m:
        yield m


class TestQuickSightSensor:
    def setup_method(self):
        self.default_op_kwargs = {
            "task_id": "quicksight_sensor",
            "aws_conn_id": None,
            "data_set_id": DATA_SET_ID,
            "ingestion_id": INGESTION_ID,
        }

    def test_init(self):
        self.default_op_kwargs.pop("aws_conn_id", None)

        sensor = QuickSightSensor(
            **self.default_op_kwargs,
            # Generic hooks parameters
            aws_conn_id="fake-conn-id",
            region_name="ca-west-1",
            verify=True,
            botocore_config={"read_timeout": 42},
        )
        assert sensor.hook.client_type == "quicksight"
        assert sensor.hook.resource_type is None
        assert sensor.hook.aws_conn_id == "fake-conn-id"
        assert sensor.hook._region_name == "ca-west-1"
        assert sensor.hook._verify is True
        assert sensor.hook._config is not None
        assert sensor.hook._config.read_timeout == 42

        sensor = QuickSightSensor(**self.default_op_kwargs)
        assert sensor.hook.aws_conn_id == "aws_default"
        assert sensor.hook._region_name is None
        assert sensor.hook._verify is None
        assert sensor.hook._config is None

    @pytest.mark.parametrize("status", ["COMPLETED"])
    def test_poke_completed(self, status, mocked_get_status):
        mocked_get_status.return_value = status
        assert QuickSightSensor(**self.default_op_kwargs).poke({}) is True
        mocked_get_status.assert_called_once_with(None, DATA_SET_ID, INGESTION_ID)

    @pytest.mark.parametrize("status", ["INITIALIZED"])
    def test_poke_not_completed(self, status, mocked_get_status):
        mocked_get_status.return_value = status
        assert QuickSightSensor(**self.default_op_kwargs).poke({}) is False
        mocked_get_status.assert_called_once_with(None, DATA_SET_ID, INGESTION_ID)

    @pytest.mark.parametrize("status", ["FAILED", "CANCELLED"])
    def test_poke_terminated_status(
        self, status, mocked_get_status, mocked_get_error_info
    ):
        mocked_get_status.return_value = status
        mocked_get_error_info.return_value = "something bad happen"
        with pytest.raises(AirflowException, match="Error info: something bad happen"):
            QuickSightSensor(**self.default_op_kwargs).poke({})
        mocked_get_status.assert_called_once_with(None, DATA_SET_ID, INGESTION_ID)
        mocked_get_error_info.assert_called_once_with(None, DATA_SET_ID, INGESTION_ID)
