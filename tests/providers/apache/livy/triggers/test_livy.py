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
from aiohttp import ClientConnectionError

from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.providers.apache.livy.triggers.livy import LivyTrigger
from airflow.triggers.base import TriggerEvent


class TestLivyTrigger:
    def test_livy_trigger_serialization(self):
        """
        Asserts that the TaskStateTrigger correctly serializes its arguments
        and classpath.
        """
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=0
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.apache.livy.triggers.livy.LivyTrigger"
        assert kwargs == {
            "batch_id": 1,
            "spark_params": {},
            "livy_conn_id": LivyHook.default_conn_name,
            "polling_interval": 0,
            "extra_options": None,
            "extra_headers": None,
            "livy_hook_async": None,
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
    async def test_livy_trigger_run_with_no_poll_interval(self, mock_poll_for_termination):
        """
        Test if the task ran in the triggerer successfully with poll interval=0.
        In the case when polling_interval=0, it should return the batch_id
        """
        mock_poll_for_termination.return_value = {"status": "success"}
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=0
        )
        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {"status": "success", "batch_id": 1, "response": "Batch 1 succeeded", "log_lines": None}
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
    async def test_livy_trigger_run_with_poll_interval_success(self, mock_poll_for_termination):
        """
        Test if the task ran in the triggerer successfully with poll interval>0. In the case when
        polling_interval > 0, it should return a success or failure status.
        """
        mock_poll_for_termination.return_value = {"status": "success"}
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        generator = trigger.run()
        actual = await generator.asend(None)
        assert TriggerEvent({"status": "success"}) == actual

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.triggers.livy.LivyTrigger.poll_for_termination")
    async def test_livy_trigger_run_with_poll_interval_error(self, mock_poll_for_termination):
        """Test if the task in the trigger returned an error when poll_for_termination returned error."""
        mock_poll_for_termination.return_value = {"status": "error"}
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        task = [i async for i in trigger.run()]
        assert len(task) == 2
        assert TriggerEvent({"status": "error"}) in task

    @pytest.mark.asyncio
    async def test_livy_trigger_run_with_exception(self):
        """Test if the task in the trigger failed with a connection error when no connection is mocked."""
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        task = [i async for i in trigger.run()]
        assert len(task) == 1
        assert (
            TriggerEvent(
                {
                    "status": "error",
                    "batch_id": 1,
                    "response": "Batch 1 did not succeed with Cannot connect to host livy:8998 ssl:default "
                    "[Name or service not known]",
                    "log_lines": None,
                }
            )
            in task
        )

    @pytest.mark.asyncio
    async def test_livy_trigger_poll_for_termination_with_client_error(self):
        """
        Test if the poll_for_termination() in the trigger failed with a ClientConnectionError
        when no connection is mocked.
        """
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        with pytest.raises(ClientConnectionError):
            await trigger.poll_for_termination(1)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.dump_batch_logs")
    async def test_livy_trigger_poll_for_termination_success(
        self, mock_dump_batch_logs, mock_get_batch_state
    ):
        """
        Test if the poll_for_termination() in the triggerer returned success response when get_batch_state()
        runs successfully.
        """
        mock_get_batch_state.return_value = {"batch_state": BatchState.SUCCESS}
        mock_dump_batch_logs.return_value = ["mock_log"]
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        task = await trigger.poll_for_termination(1)

        assert task == {
            "status": "success",
            "batch_id": 1,
            "response": "Batch 1 succeeded",
            "log_lines": ["mock_log"],
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.dump_batch_logs")
    async def test_livy_trigger_poll_for_termination_error(self, mock_dump_batch_logs, mock_get_batch_state):
        """
        Test if the poll_for_termination() in the trigger returned error response when get_batch_state()
        failed.
        """
        mock_get_batch_state.return_value = {"batch_state": BatchState.ERROR}
        mock_dump_batch_logs.return_value = ["mock_log"]
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        task = await trigger.poll_for_termination(1)

        assert task == {
            "status": "error",
            "batch_id": 1,
            "response": "Batch 1 did not succeed",
            "log_lines": ["mock_log"],
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.get_batch_state")
    @mock.patch("airflow.providers.apache.livy.hooks.livy.LivyAsyncHook.dump_batch_logs")
    async def test_livy_trigger_poll_for_termination_state(self, mock_dump_batch_logs, mock_get_batch_state):
        """
        Test if the poll_for_termination() in the trigger is still polling when get_batch_state() returned
        NOT_STARTED.
        """
        mock_get_batch_state.return_value = {"batch_state": BatchState.NOT_STARTED}
        mock_dump_batch_logs.return_value = ["mock_log"]
        trigger = LivyTrigger(
            batch_id=1, spark_params={}, livy_conn_id=LivyHook.default_conn_name, polling_interval=30
        )

        task = asyncio.create_task(trigger.poll_for_termination(1))
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()
