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
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.triggers.ec2 import EC2StateSensorTrigger
from airflow.triggers.base import TriggerEvent

TEST_INSTANCE_ID = "test-instance-id"
TEST_TARGET_STATE = "test_state"
TEST_CONN_ID = "test_conn_id"
TEST_REGION_NAME = "test-region"
TEST_POLL_INTERVAL = 100


class TestEC2StateSensorTrigger:
    def test_ec2_state_sensor_trigger_serialize(self):
        test_ec2_state_sensor = EC2StateSensorTrigger(
            instance_id=TEST_INSTANCE_ID,
            target_state=TEST_TARGET_STATE,
            aws_conn_id=TEST_CONN_ID,
            region_name=TEST_REGION_NAME,
            poll_interval=TEST_POLL_INTERVAL,
        )

        class_path, args = test_ec2_state_sensor.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.ec2.EC2StateSensorTrigger"
        )
        assert args["instance_id"] == TEST_INSTANCE_ID
        assert args["target_state"] == TEST_TARGET_STATE
        assert args["aws_conn_id"] == TEST_CONN_ID
        assert args["region_name"] == TEST_REGION_NAME
        assert args["poll_interval"] == TEST_POLL_INTERVAL

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.ec2.EC2Hook.get_instance_state_async")
    @mock.patch("airflow.providers.amazon.aws.hooks.ec2.EC2Hook.async_conn")
    async def test_ec2_state_sensor_run(
        self, mock_async_conn, mock_get_instance_state_async
    ):
        mock = AsyncMock()
        mock_async_conn.__aenter__.return_value = mock
        mock_get_instance_state_async.return_value = TEST_TARGET_STATE

        test_ec2_state_sensor = EC2StateSensorTrigger(
            instance_id=TEST_INSTANCE_ID,
            target_state=TEST_TARGET_STATE,
            aws_conn_id=TEST_CONN_ID,
            region_name=TEST_REGION_NAME,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = test_ec2_state_sensor.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "message": "target state met"}
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.ec2.EC2Hook.get_instance_state_async")
    @mock.patch("airflow.providers.amazon.aws.hooks.ec2.EC2Hook.async_conn")
    async def test_ec2_state_sensor_run_multiple(
        self, mock_async_conn, mock_get_instance_state_async, mock_sleep
    ):
        mock = AsyncMock()
        mock_async_conn.__aenter__.return_value = mock
        mock_get_instance_state_async.side_effect = ["test-state", TEST_TARGET_STATE]
        mock_sleep.return_value = True

        test_ec2_state_sensor = EC2StateSensorTrigger(
            instance_id=TEST_INSTANCE_ID,
            target_state=TEST_TARGET_STATE,
            aws_conn_id=TEST_CONN_ID,
            region_name=TEST_REGION_NAME,
            poll_interval=TEST_POLL_INTERVAL,
        )

        generator = test_ec2_state_sensor.run()
        response = await generator.asend(None)

        assert mock_get_instance_state_async.call_count == 2

        assert response == TriggerEvent(
            {"status": "success", "message": "target state met"}
        )
