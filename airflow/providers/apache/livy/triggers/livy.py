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

"""This module contains the Apache Livy Trigger."""
from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator

from airflow.providers.apache.livy.hooks.livy import BatchState, LivyAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class LivyTrigger(BaseTrigger):
    """
    Check for the state of a previously submitted job with batch_id

    :param batch_id: Batch job id
    :param spark_params: Spark parameters; for example,
            spark_params = {"file": "test/pi.py", "class_name": "org.apache.spark.examples.SparkPi",
            "args": ["/usr/lib/spark/bin/run-example", "SparkPi", "10"],"jars": "command-runner.jar",
            "driver_cores": 1, "executor_cores": 4,"num_executors": 1}
    :param livy_conn_id: reference to a pre-defined Livy Connection.
    :param polling_interval: time in seconds between polling for job completion.  If poll_interval=0, in that
        case return the batch_id and if polling_interval > 0, poll the livy job for termination in the
        polling interval defined.
    :param extra_options: A dictionary of options, where key is string and value
        depends on the option that's being modified.
    :param extra_headers: A dictionary of headers passed to the HTTP request to livy.
    :param livy_hook_async: LivyAsyncHook object
    """

    def __init__(
        self,
        batch_id: int | str,
        spark_params: dict[Any, Any],
        livy_conn_id: str = "livy_default",
        polling_interval: int = 0,
        extra_options: dict[str, Any] | None = None,
        extra_headers: dict[str, Any] | None = None,
        livy_hook_async: LivyAsyncHook | None = None,
    ):
        super().__init__()
        self._batch_id = batch_id
        self.spark_params = spark_params
        self._livy_conn_id = livy_conn_id
        self._polling_interval = polling_interval
        self._extra_options = extra_options
        self._extra_headers = extra_headers
        self._livy_hook_async = livy_hook_async

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes LivyTrigger arguments and classpath."""
        return (
            "airflow.providers.apache.livy.triggers.livy.LivyTrigger",
            {
                "batch_id": self._batch_id,
                "spark_params": self.spark_params,
                "livy_conn_id": self._livy_conn_id,
                "polling_interval": self._polling_interval,
                "extra_options": self._extra_options,
                "extra_headers": self._extra_headers,
                "livy_hook_async": self._livy_hook_async,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """
        Checks if the _polling_interval > 0, in that case it pools Livy for
        batch termination asynchronously.
        else returns the success response
        """
        try:
            if self._polling_interval > 0:
                response = await self.poll_for_termination(self._batch_id)
                yield TriggerEvent(response)
            yield TriggerEvent(
                {
                    "status": "success",
                    "batch_id": self._batch_id,
                    "response": f"Batch {self._batch_id} succeeded",
                    "log_lines": None,
                }
            )
        except Exception as exc:
            yield TriggerEvent(
                {
                    "status": "error",
                    "batch_id": self._batch_id,
                    "response": f"Batch {self._batch_id} did not succeed with {str(exc)}",
                    "log_lines": None,
                }
            )

    async def poll_for_termination(self, batch_id: int | str) -> dict[str, Any]:
        """
        Pool Livy for batch termination asynchronously.

        :param batch_id: id of the batch session to monitor.
        """
        hook = self._get_async_hook()
        state = await hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s is in state: %s", batch_id, state["batch_state"].value)
        while state["batch_state"] not in hook.TERMINAL_STATES:
            self.log.info("Batch with id %s is in state: %s", batch_id, state["batch_state"].value)
            self.log.info("Sleeping for %s seconds", self._polling_interval)
            await asyncio.sleep(self._polling_interval)
            state = await hook.get_batch_state(batch_id)
        self.log.info("Batch with id %s terminated with state: %s", batch_id, state["batch_state"].value)
        log_lines = await hook.dump_batch_logs(batch_id)
        if state["batch_state"] != BatchState.SUCCESS:
            return {
                "status": "error",
                "batch_id": batch_id,
                "response": f"Batch {batch_id} did not succeed",
                "log_lines": log_lines,
            }
        return {
            "status": "success",
            "batch_id": batch_id,
            "response": f"Batch {batch_id} succeeded",
            "log_lines": log_lines,
        }

    def _get_async_hook(self) -> LivyAsyncHook:
        if self._livy_hook_async is None or not isinstance(self._livy_hook_async, LivyAsyncHook):
            self._livy_hook_async = LivyAsyncHook(
                livy_conn_id=self._livy_conn_id,
                extra_headers=self._extra_headers,
                extra_options=self._extra_options,
            )
        return self._livy_hook_async
