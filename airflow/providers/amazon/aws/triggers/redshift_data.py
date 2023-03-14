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

from typing import Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RedshiftDataTrigger(BaseTrigger):
    """
    RedshiftDataTrigger is fired as deferred class with params to run the task in triggerer.

    :param task_id: task ID of the Dag
    :param poll_interval:  polling period in seconds to check for the status
    :param aws_conn_id: AWS connection ID for redshift
    :param query_ids: list of query IDs to run and poll for the status
    """

    def __init__(
        self,
        task_id: str,
        poll_interval: int,
        query_ids: list[str],
        aws_conn_id: str = "aws_default",
    ):
        super().__init__()
        self.task_id = task_id
        self.poll_interval = poll_interval
        self.aws_conn_id = aws_conn_id
        self.query_ids = query_ids

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes RedshiftDataTrigger arguments and classpath."""
        return (
            "airflow.providers.amazon.aws.triggers.redshift_data.RedshiftDataTrigger",
            {
                "task_id": self.task_id,
                "poll_interval": self.poll_interval,
                "aws_conn_id": self.aws_conn_id,
                "query_ids": self.query_ids,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:
        """
        Makes async connection and gets status for a list of queries submitted by the operator.
        Even if one of the queries has a non-successful state, the hook returns a failure event and the error
        is sent back to the operator.
        """
        hook = RedshiftDataAsyncHook(aws_conn_id=self.aws_conn_id, poll_interval=self.poll_interval)
        try:
            response = await hook.get_query_status(self.query_ids)
            if response:
                yield TriggerEvent(response)
            else:
                error_message = f"{self.task_id} failed"
                yield TriggerEvent({"status": "error", "message": error_message})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
