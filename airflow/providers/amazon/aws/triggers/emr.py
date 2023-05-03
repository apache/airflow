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

from typing import Any, AsyncIterator, Iterable

from airflow.compat.functools import cached_property
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class EmrStepSensorTrigger(BaseTrigger):
    """
    Poll for the status of EMR container until reaches terminal state

    :param virtual_cluster_id: Reference Emr cluster id
    :param job_id:  job_id to check the state
    :param max_tries: maximum try attempts for polling the status
    :param aws_conn_id: Reference to AWS connection id
    :param poll_interval: polling period in seconds to check for the status
    """

    def __init__(
        self,
        job_flow_id: str,
        step_id: str,
        target_states: Iterable[str],
        aws_conn_id: str = "aws_default",
        poll_interval: int = 30,
        max_attempts: int = 60,
        **kwargs: Any,
    ):
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states
        self.aws_conn_id = aws_conn_id
        self.poll_interval = poll_interval
        self.max_attempts = max_attempts
        super().__init__(**kwargs)

    @cached_property
    def hook(self) -> EmrHook:
        return EmrHook(self.aws_conn_id)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.amazon.aws.triggers.emr.EmrStepSensorTrigger",
            {
                "job_flow_id": self.job_flow_id,
                "step_id": self.step_id,
                "target_states": self.target_states,
                "aws_conn_id": self.aws_conn_id,
                "max_attempts": self.max_attempts,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        async with self.hook.async_conn as client:
            waiter = self.hook.get_waiter("job_step_wait_for_terminal", deferrable=True, client=client)
            await waiter.wait(
                ClusterId=self.job_flow_id,
                StepId=self.step_id,
                WaiterConfig={
                    "Delay": self.poll_interval,
                    "MaxAttempts": self.max_attempts,
                },
            )

        response = self.hook.conn.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)
        state = response["Step"]["Status"]["State"]
        if state in self.target_states:
            yield TriggerEvent({"status": "success"})
        else:
            yield TriggerEvent({"status": "failed", "response": response})
