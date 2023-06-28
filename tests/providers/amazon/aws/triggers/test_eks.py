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
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateFargateProfileTrigger,
    EksDeleteFargateProfileTrigger,
    EksNodegroupTrigger,
)
from airflow.triggers.base import TriggerEvent

TEST_CLUSTER_IDENTIFIER = "test-cluster"
TEST_FARGATE_PROFILE_NAME = "test-fargate-profile"
TEST_NODEGROUP_NAME = "test-nodegroup"
TEST_WAITER_DELAY = 10
TEST_WAITER_MAX_ATTEMPTS = 10
TEST_AWS_CONN_ID = "test-aws-id"
TEST_REGION = "test-region"


class TestEksCreateFargateProfileTrigger:
    def test_eks_create_fargate_profile_serialize(self):
        eks_create_fargate_profile_trigger = EksCreateFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        class_path, args = eks_create_fargate_profile_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.eks.EksCreateFargateProfileTrigger"
        assert args["cluster_name"] == TEST_CLUSTER_IDENTIFIER
        assert args["fargate_profile_name"] == TEST_FARGATE_PROFILE_NAME
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["waiter_delay"] == str(TEST_WAITER_DELAY)
        assert args["waiter_max_attempts"] == str(TEST_WAITER_MAX_ATTEMPTS)

    @pytest.mark.asyncio
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_create_fargate_profile_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock.get_waiter().wait = AsyncMock()

        eks_create_fargate_profile_trigger = EksCreateFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        generator = eks_create_fargate_profile_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Fargate Profile Created"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_create_fargate_profile_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "CREATING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        eks_create_fargate_profile_trigger = EksCreateFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        generator = eks_create_fargate_profile_trigger.run()
        response = await generator.asend(None)

        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Fargate Profile Created"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_create_fargate_profile_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "CREATING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        eks_create_fargate_profile_trigger = EksCreateFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=2,
        )
        with pytest.raises(AirflowException) as exc:
            generator = eks_create_fargate_profile_trigger.run()
            await generator.asend(None)
        assert "Create Fargate Profile failed - max attempts reached:" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_create_fargate_profile_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_creating = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "CREATING"}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"fargateProfile": {"status": "CREATE_FAILED"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error_creating, error_creating, error_failed])
        mock_sleep.return_value = True

        eks_create_fargate_profile_trigger = EksCreateFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        with pytest.raises(AirflowException) as exc:
            generator = eks_create_fargate_profile_trigger.run()
            await generator.asend(None)
        assert f"Create Fargate Profile failed: {error_failed}" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 3


class TestEksDeleteFargateProfileTrigger:
    def test_eks_delete_fargate_profile_serialize(self):
        eks_delete_fargate_profile_trigger = EksDeleteFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        class_path, args = eks_delete_fargate_profile_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.eks.EksDeleteFargateProfileTrigger"
        assert args["cluster_name"] == TEST_CLUSTER_IDENTIFIER
        assert args["fargate_profile_name"] == TEST_FARGATE_PROFILE_NAME
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["waiter_delay"] == str(TEST_WAITER_DELAY)
        assert args["waiter_max_attempts"] == str(TEST_WAITER_MAX_ATTEMPTS)

    @pytest.mark.asyncio
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_delete_fargate_profile_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock.get_waiter().wait = AsyncMock()

        eks_delete_fargate_profile_trigger = EksDeleteFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        generator = eks_delete_fargate_profile_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Fargate Profile Deleted"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_delete_fargate_profile_trigger_run_multiple_attempts(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "DELETING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, True])
        mock_sleep.return_value = True

        eks_delete_fargate_profile_trigger = EksDeleteFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )

        generator = eks_delete_fargate_profile_trigger.run()
        response = await generator.asend(None)
        assert a_mock.get_waiter().wait.call_count == 3
        assert response == TriggerEvent({"status": "success", "message": "Fargate Profile Deleted"})

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_delete_fargate_profile_trigger_run_attempts_exceeded(
        self, mock_async_conn, mock_sleep
    ):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "DELETING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, error, True])
        mock_sleep.return_value = True

        eks_delete_fargate_profile_trigger = EksDeleteFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=2,
        )
        with pytest.raises(AirflowException) as exc:
            generator = eks_delete_fargate_profile_trigger.run()
            await generator.asend(None)
        assert "Delete Fargate Profile failed - max attempts reached: 2" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_delete_fargate_profile_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_creating = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"fargateProfile": {"status": "DELETING"}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"fargateProfile": {"status": "DELETE_FAILED"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error_creating, error_creating, error_failed])
        mock_sleep.return_value = True

        eks_delete_fargate_profile_trigger = EksDeleteFargateProfileTrigger(
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
        )
        with pytest.raises(AirflowException) as exc:
            generator = eks_delete_fargate_profile_trigger.run()
            await generator.asend(None)
        assert f"Delete Fargate Profile failed: {error_failed}" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 3


class TestEksNodegroupTrigger:
    def test_eks_nodegroup_trigger_serialize(self):
        eks_nodegroup_trigger = EksNodegroupTrigger(
            waiter_name="test_waiter_name",
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            nodegroup_name=TEST_NODEGROUP_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            region=TEST_REGION,
        )

        class_path, args = eks_nodegroup_trigger.serialize()
        assert class_path == "airflow.providers.amazon.aws.triggers.eks.EksNodegroupTrigger"
        assert args["waiter_name"] == "test_waiter_name"
        assert args["cluster_name"] == TEST_CLUSTER_IDENTIFIER
        assert args["nodegroup_name"] == TEST_NODEGROUP_NAME
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID
        assert args["waiter_delay"] == str(TEST_WAITER_DELAY)
        assert args["waiter_max_attempts"] == str(TEST_WAITER_MAX_ATTEMPTS)
        assert args["region"] == TEST_REGION

    @pytest.mark.asyncio
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_nodegroup_trigger_run(self, mock_async_conn):
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock

        a_mock.get_waiter().wait = AsyncMock()

        eks_nodegroup_trigger = EksNodegroupTrigger(
            waiter_name="test_waiter_name",
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            nodegroup_name=TEST_NODEGROUP_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            region=TEST_REGION,
        )

        generator = eks_nodegroup_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {
                "status": "success",
                "cluster_name": TEST_CLUSTER_IDENTIFIER,
                "nodegroup_name": TEST_NODEGROUP_NAME,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_nodegroup_trigger_run_multiple_attempts(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"nodegroup": {"status": "CREATING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, error, True])

        eks_nodegroup_trigger = EksNodegroupTrigger(
            waiter_name="test_waiter_name",
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            nodegroup_name=TEST_NODEGROUP_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            region=TEST_REGION,
        )

        generator = eks_nodegroup_trigger.run()
        response = await generator.asend(None)
        assert a_mock.get_waiter().wait.call_count == 4
        assert response == TriggerEvent(
            {
                "status": "success",
                "cluster_name": TEST_CLUSTER_IDENTIFIER,
                "nodegroup_name": TEST_NODEGROUP_NAME,
            }
        )

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_nodegroup_trigger_run_attempts_exceeded(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"nodegroup": {"status": "CREATING"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error, error, error, True])

        eks_nodegroup_trigger = EksNodegroupTrigger(
            waiter_name="test_waiter_name",
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            nodegroup_name=TEST_NODEGROUP_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=2,
            region=TEST_REGION,
        )

        with pytest.raises(AirflowException) as exc:
            generator = eks_nodegroup_trigger.run()
            await generator.asend(None)
        assert "Waiter error: max attempts reached" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 2

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch.object(EksHook, "async_conn")
    async def test_eks_nodegroup_trigger_run_attempts_failed(self, mock_async_conn, mock_sleep):
        mock_sleep.return_value = True
        a_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = a_mock
        error_creating = WaiterError(
            name="test_name",
            reason="test_reason",
            last_response={"nodegroup": {"status": "CREATING"}},
        )
        error_failed = WaiterError(
            name="test_name",
            reason="Waiter encountered a terminal failure state:",
            last_response={"nodegroup": {"status": "DELETE_FAILED"}},
        )
        a_mock.get_waiter().wait = AsyncMock(side_effect=[error_creating, error_creating, error_failed])
        mock_sleep.return_value = True

        eks_nodegroup_trigger = EksNodegroupTrigger(
            waiter_name="test_waiter_name",
            cluster_name=TEST_CLUSTER_IDENTIFIER,
            nodegroup_name=TEST_NODEGROUP_NAME,
            aws_conn_id=TEST_AWS_CONN_ID,
            waiter_delay=TEST_WAITER_DELAY,
            waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            region=TEST_REGION,
        )
        with pytest.raises(AirflowException) as exc:
            generator = eks_nodegroup_trigger.run()
            await generator.asend(None)

        assert "Error checking nodegroup" in str(exc.value)
        assert a_mock.get_waiter().wait.call_count == 3
