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
from botocore.exceptions import WaiterError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.triggers.rds import RdsDbInstanceTrigger
from airflow.triggers.base import TriggerEvent

TEST_DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier"
TEST_WAITER_DELAY = 10
TEST_WAITER_MAX_ATTEMPTS = 10
TEST_AWS_CONN_ID = "test-aws-id"
TEST_REGION = "sa-east-1"
TEST_RESPONSE = {
    "DBInstance": {
        "DBInstanceIdentifier": "test-db-instance-identifier",
        "DBInstanceStatus": "test-db-instance-status",
    }
}


class TestRdsDbInstanceTrigger:
    def test_rds_db_instance_trigger_serialize(self):
        rds_db_instance_trigger = RdsDbInstanceTrigger(
            waiter_name="test-waiter",
            db_instance_identifier=TEST_DB_INSTANCE_IDENTIFIER,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION,
            response=TEST_RESPONSE,
        )
        class_path, args = rds_db_instance_trigger.serialize()

        assert class_path == "airflow.providers.amazon.aws.triggers.rds.RdsDbInstanceTrigger"
        assert args["waiter_name"] == "test-waiter"
        assert args["db_instance_identifier"] == TEST_DB_INSTANCE_IDENTIFIER
        assert args["waiter_delay"] == str(TEST_WAITER_DELAY)
        assert args["waiter_max_attempts"] == str(TEST_WAITER_MAX_ATTEMPTS)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["region_name"] == TEST_REGION
        assert args["response"] == TEST_RESPONSE

    @pytest.mark.asyncio
    @mock.patch.object(RdsHook, "async_conn")
    async def test_rds_db_instance_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock.get_waiter().wait = AsyncMock()

        rds_db_instance_trigger = RdsDbInstanceTrigger(
            waiter_name="test-waiter",
            db_instance_identifier=TEST_DB_INSTANCE_IDENTIFIER,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION,
            response=TEST_RESPONSE,
        )

        generator = rds_db_instance_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "response": TEST_RESPONSE})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RdsHook, "async_conn")
    async def test_rds_db_instance_trigger_run_multiple_attempts(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"DBInstances": [{"DBInstanceStatus": "CREATING"}]},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, error, True])

        rds_db_instance_trigger = RdsDbInstanceTrigger(
            waiter_name="test-waiter",
            db_instance_identifier=TEST_DB_INSTANCE_IDENTIFIER,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION,
            response=TEST_RESPONSE,
        )

        generator = rds_db_instance_trigger.run()
        response = await generator.asend(None)
        assert a_mock.get_waiter().wait.call_count == 4

        assert response == TriggerEvent({"status": "success", "response": TEST_RESPONSE})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RdsHook, "async_conn")
    async def test_rds_db_instance_trigger_run_attempts_exceeded(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True

        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"DBInstances": [{"DBInstanceStatus": "CREATING"}]},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, error, True])

        rds_db_instance_trigger = RdsDbInstanceTrigger(
            waiter_name="test-waiter",
            db_instance_identifier=TEST_DB_INSTANCE_IDENTIFIER,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=2,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION,
            response=TEST_RESPONSE,
        )

        with pytest.raises(AirflowException) as exc:
            generator = rds_db_instance_trigger.run()
            await generator.asend(None)

        assert "Waiter error: max attempts reached" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(RdsHook, "async_conn")
    async def test_rds_db_instance_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        error_creating = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"DBInstances": [{"DBInstanceStatus": "CREATING"}]},
        )

        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"DBInstances": [{"DBInstanceStatus": "FAILED"}]},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error_creating, error_creating, error_failed])
        mock_sleep.return_value = True

        rds_db_instance_trigger = RdsDbInstanceTrigger(
            waiter_name="test-waiter",
            db_instance_identifier=TEST_DB_INSTANCE_IDENTIFIER,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            aws_conn_id=TEST_AWS_CONN_ID,
            region_name=TEST_REGION,
            response=TEST_RESPONSE,
        )

        with pytest.raises(AirflowException) as exc:
            generator = rds_db_instance_trigger.run()
            await generator.asend(None)
        assert "Error checking DB Instance status" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 3
