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
from datetime import datetime
from unittest import mock as async_mock

import pytest

from airflow.providers.amazon.aws.triggers.s3 import S3KeysUnchangedTrigger, S3KeyTrigger
from airflow.triggers.base import TriggerEvent


class TestS3KeyTrigger:
    def test_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeyTrigger(
            bucket_key="s3://test_bucket/file", bucket_name="test_bucket", wildcard_match=True
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeyTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "bucket_key": "s3://test_bucket/file",
            "wildcard_match": True,
            "aws_conn_id": "aws_default",
            "hook_params": {},
            "poke_interval": 5.0,
            "should_check_fn": False,
            "use_regex": False,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_run_success(self, mock_client):
        """
        Test if the task is run is in triggerr successfully.
        """
        mock_client.return_value.check_key.return_value = True
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is True
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.check_key_async")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_run_pending(self, mock_client, mock_check_key_async):
        """
        Test if the task is run is in trigger successfully and set check_key to return false.
        """
        mock_check_key_async.return_value = False
        trigger = S3KeyTrigger(bucket_key="s3://test_bucket/file", bucket_name="test_bucket")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert task.done() is False
        asyncio.get_event_loop().stop()


class TestS3KeysUnchangedTrigger:
    def test_serialization(self):
        """
        Asserts that the S3KeysUnchangedTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = S3KeysUnchangedTrigger(
            bucket_name="test_bucket",
            prefix="test",
            inactivity_period=1,
            min_objects=1,
            inactivity_seconds=0,
            previous_objects=None,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.s3.S3KeysUnchangedTrigger"
        assert kwargs == {
            "bucket_name": "test_bucket",
            "prefix": "test",
            "inactivity_period": 1,
            "min_objects": 1,
            "inactivity_seconds": 0,
            "previous_objects": set(),
            "allow_delete": True,
            "aws_conn_id": "aws_default",
            "last_activity_time": None,
            "hook_params": {},
            "verify": None,
            "polling_period_seconds": 0,
        }

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    async def test_run_wait(self, mock_client):
        """Test if the task is run in trigger successfully."""
        mock_client.return_value.check_key.return_value = True
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        with mock_client:
            task = asyncio.create_task(trigger.run().__anext__())
            await asyncio.sleep(0.5)

            assert task.done() is True
            asyncio.get_event_loop().stop()

    def test_run_raise_value_error(self):
        """
        Test if the S3KeysUnchangedTrigger raises Value error for negative inactivity_period.
        """
        with pytest.raises(ValueError):
            S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test", inactivity_period=-100)

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.is_keys_unchanged_async")
    async def test_run_success(self, mock_is_keys_unchanged, mock_client):
        """
        Test if the task is run in triggerer successfully.
        """
        mock_is_keys_unchanged.return_value = {"status": "success"}
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success"}) == actual

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.async_conn")
    @async_mock.patch("airflow.providers.amazon.aws.triggers.s3.S3Hook.is_keys_unchanged_async")
    async def test_run_pending(self, mock_is_keys_unchanged, mock_client):
        """Test if the task is run in triggerer successfully."""
        mock_is_keys_unchanged.return_value = {"status": "pending", "last_activity_time": datetime.now()}
        trigger = S3KeysUnchangedTrigger(bucket_name="test_bucket", prefix="test")
        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()
