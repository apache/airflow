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

from typing import Any

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class AthenaTrigger(BaseTrigger):
    """
    Trigger for RedshiftCreateClusterOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    Redshift cluster to be in the `available` state.

    :param query_execution_id:  ID of the Athena query execution to watch
    :param poll_interval: The amount of time in seconds to wait between attempts.
    :param max_attempt: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        query_execution_id: str,
        poll_interval: int,
        max_attempt: int,
        aws_conn_id: str,
    ):
        self.query_execution_id = query_execution_id
        self.poll_interval = poll_interval
        self.max_attempt = max_attempt
        self.aws_conn_id = aws_conn_id

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "query_execution_id": str(self.query_execution_id),
                "poll_interval": str(self.poll_interval),
                "max_attempt": str(self.max_attempt),
                "aws_conn_id": str(self.aws_conn_id),
            },
        )

    async def run(self):
        hook = AthenaHook(self.aws_conn_id)
        async with hook.async_conn as client:
            waiter = hook.get_waiter("query_complete", deferrable=True, client=client)
            await async_wait(
                waiter=waiter,
                waiter_delay=self.poll_interval,
                waiter_max_attempts=self.max_attempt,
                args={"QueryExecutionId": self.query_execution_id},
                failure_message=f"Error while waiting for query {self.query_execution_id} to complete",
                status_message=f"Query execution id: {self.query_execution_id}, "
                "Query is still in non-terminal state",
                status_args=["QueryExecution.Status.State"],
            )
        yield TriggerEvent({"status": "success", "value": self.query_execution_id})
