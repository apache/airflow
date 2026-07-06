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

import asyncio
from unittest import mock

import pytest

from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSettledTrigger,
    RedshiftClusterTrigger,
    RedshiftCreateClusterSnapshotTrigger,
    RedshiftCreateClusterTrigger,
    RedshiftDeleteClusterTrigger,
    RedshiftPauseClusterTrigger,
    RedshiftResumeClusterTrigger,
)
from airflow.triggers.base import TriggerEvent

POLLING_PERIOD_SECONDS = 1.0


class TestRedshiftClusterTrigger:
    def test_redshift_cluster_sensor_trigger_serialization(self):
        """
        Asserts that the RedshiftClusterTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterTrigger"
        assert kwargs == {
            "aws_conn_id": "test_redshift_conn_id",
            "cluster_identifier": "mock_cluster_identifier",
            "target_status": "available",
            "poke_interval": POLLING_PERIOD_SECONDS,
            "region_name": None,
            "verify": None,
            "botocore_config": None,
        }

    def test_redshift_cluster_trigger_serializes_generic_hook_params(self):
        """Asserts the generic AWS hook params are serialized and used to build the hook."""
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        _, kwargs = trigger.serialize()
        assert kwargs["region_name"] == "eu-west-1"
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"read_timeout": 42}

        hook = trigger.hook
        assert hook.aws_conn_id == "test_redshift_conn_id"
        assert hook._region_name == "eu-west-1"
        assert hook._verify is False
        assert hook._config.read_timeout == 42

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_redshift_cluster_sensor_trigger_success(self, mock_cluster_status):
        """
        Test RedshiftClusterTrigger with the success status
        """
        expected_result = "available"

        mock_cluster_status.return_value = expected_result
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success", "message": "target state met"}) == actual

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "expected_result",
        ["Resuming"],
    )
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_redshift_cluster_sensor_trigger_resuming_status(
        self, mock_cluster_status, expected_result
    ):
        """Test RedshiftClusterTrigger with the success status"""
        mock_cluster_status.return_value = expected_result
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was returned
        assert task.done() is False

        # Prevents error when task is destroyed while in "pending" state
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_redshift_cluster_sensor_trigger_exception(self, mock_cluster_status):
        """Test RedshiftClusterTrigger with exception"""
        mock_cluster_status.side_effect = Exception("Test exception")
        trigger = RedshiftClusterTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            target_status="available",
            poke_interval=POLLING_PERIOD_SECONDS,
        )

        task = [i async for i in trigger.run()]
        # since we use return as soon as we yield the trigger event
        # at any given point there should be one trigger event returned to the task
        # so we validate for length of task to be 1
        assert len(task) == 1
        assert TriggerEvent({"status": "error", "message": "Test exception"}) in task


class TestRedshiftClusterSettledTrigger:
    def test_serialization(self):
        """Asserts that RedshiftClusterSettledTrigger serializes its arguments and classpath."""
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
            max_attempts=42,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == (
            "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftClusterSettledTrigger"
        )
        assert kwargs == {
            "aws_conn_id": "test_redshift_conn_id",
            "cluster_identifier": "mock_cluster_identifier",
            "poke_interval": POLLING_PERIOD_SECONDS,
            "max_attempts": 42,
            "region_name": None,
            "verify": None,
            "botocore_config": None,
        }

    def test_serializes_generic_hook_params(self):
        """Asserts the generic AWS hook params are serialized and used to build the hook."""
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        _, kwargs = trigger.serialize()
        assert kwargs["region_name"] == "eu-west-1"
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"read_timeout": 42}

        hook = trigger.hook
        assert hook.aws_conn_id == "test_redshift_conn_id"
        assert hook._region_name == "eu-west-1"
        assert hook._verify is False
        assert hook._config.read_timeout == 42

    @pytest.mark.asyncio
    @pytest.mark.parametrize("settled_status", ["available", "paused", "cluster_not_found"])
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_fires_when_settled(self, mock_cluster_status, settled_status):
        """Fires success as soon as the cluster leaves every transitional state."""
        mock_cluster_status.return_value = settled_status
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        actual = await trigger.run().asend(None)
        assert actual == TriggerEvent(
            {"status": "success", "message": "Cluster settled", "cluster_state": settled_status}
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_fires_when_cluster_gone(self, mock_cluster_status):
        """A missing cluster (status None) counts as settled."""
        mock_cluster_status.return_value = None
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        actual = await trigger.run().asend(None)
        assert actual.payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_keeps_polling_while_transitional(self, mock_cluster_status, mock_sleep):
        """Keeps polling while the cluster is transitional, then fires when it settles."""
        mock_cluster_status.side_effect = ["pausing", "pausing", "paused"]
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        actual = await trigger.run().asend(None)
        assert actual.payload["status"] == "success"
        assert actual.payload["cluster_state"] == "paused"
        assert mock_cluster_status.call_count == 3

    @pytest.mark.asyncio
    @mock.patch("asyncio.sleep")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_errors_after_max_attempts(self, mock_cluster_status, mock_sleep):
        """Emits an error event if the cluster never settles within max_attempts."""
        mock_cluster_status.return_value = "resizing"
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
            max_attempts=3,
        )
        actual = await trigger.run().asend(None)
        assert actual.payload["status"] == "error"
        assert mock_cluster_status.call_count == 3

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status_async")
    async def test_error_on_exception(self, mock_cluster_status):
        """Emits an error event when polling raises."""
        mock_cluster_status.side_effect = Exception("boom")
        trigger = RedshiftClusterSettledTrigger(
            aws_conn_id="test_redshift_conn_id",
            cluster_identifier="mock_cluster_identifier",
            poke_interval=POLLING_PERIOD_SECONDS,
        )
        actual = await trigger.run().asend(None)
        assert actual == TriggerEvent({"status": "error", "message": "boom"})


WAITER_TRIGGER_PARAMS = [
    pytest.param(
        RedshiftCreateClusterTrigger,
        15,
        999999,
        id="RedshiftCreateClusterTrigger",
    ),
    pytest.param(
        RedshiftPauseClusterTrigger,
        15,
        999999,
        id="RedshiftPauseClusterTrigger",
    ),
    pytest.param(
        RedshiftCreateClusterSnapshotTrigger,
        15,
        999999,
        id="RedshiftCreateClusterSnapshotTrigger",
    ),
    pytest.param(
        RedshiftResumeClusterTrigger,
        15,
        999999,
        id="RedshiftResumeClusterTrigger",
    ),
    pytest.param(
        RedshiftDeleteClusterTrigger,
        30,
        30,
        id="RedshiftDeleteClusterTrigger",
    ),
]


class TestRedshiftWaiterTriggers:
    """Tests for the five Redshift triggers that inherit from ``AwsBaseWaiterTrigger``."""

    @pytest.mark.parametrize(
        ("trigger_cls", "default_delay", "default_max_attempts"),
        WAITER_TRIGGER_PARAMS,
    )
    def test_serialization(self, trigger_cls, default_delay, default_max_attempts):
        trigger = trigger_cls(
            cluster_identifier="test_cluster",
            aws_conn_id="aws_default",
            region_name="us-east-1",
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == f"airflow.providers.amazon.aws.triggers.redshift_cluster.{trigger_cls.__name__}"
        assert kwargs == {
            "cluster_identifier": "test_cluster",
            "waiter_delay": default_delay,
            "waiter_max_attempts": default_max_attempts,
            "aws_conn_id": "aws_default",
            "region_name": "us-east-1",
        }

    @pytest.mark.parametrize(
        ("trigger_cls", "default_delay", "default_max_attempts"),
        WAITER_TRIGGER_PARAMS,
    )
    def test_serialization_with_verify_and_botocore_config(
        self, trigger_cls, default_delay, default_max_attempts
    ):
        trigger = trigger_cls(
            cluster_identifier="test_cluster",
            aws_conn_id="aws_default",
            verify=False,
            botocore_config={"connect_timeout": 30},
        )

        _, kwargs = trigger.serialize()
        assert kwargs["verify"] is False
        assert kwargs["botocore_config"] == {"connect_timeout": 30}
        assert "region_name" not in kwargs

    @pytest.mark.parametrize(
        ("trigger_cls", "default_delay", "default_max_attempts"),
        WAITER_TRIGGER_PARAMS,
    )
    @mock.patch("airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftHook")
    def test_hook_propagates_verify_and_botocore_config(
        self, mock_hook_cls, trigger_cls, default_delay, default_max_attempts
    ):
        trigger = trigger_cls(
            cluster_identifier="test_cluster",
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            botocore_config={"read_timeout": 60},
        )

        trigger.hook()

        mock_hook_cls.assert_called_once_with(
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            config={"read_timeout": 60},
        )
