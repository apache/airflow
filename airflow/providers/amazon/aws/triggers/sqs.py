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
from typing import Any, Collection, Literal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.utils.sqs import process_response
from airflow.triggers.base import BaseTrigger, TriggerEvent


class SqsSensorTrigger(BaseTrigger):
    """Trigger for SqsSensor."""

    def __init__(
        self,
        sqs_queue,
        aws_conn_id: str = "aws_default",
        max_messages: int = 5,
        num_batches: int = 1,
        wait_time_seconds: int = 1,
        visibility_timeout: int | None = None,
        message_filtering: Literal["literal", "jsonpath"] | None = None,
        message_filtering_match_values: Any = None,
        message_filtering_config: Any = None,
        delete_message_on_reception: bool = True,
        waiter_delay: int = 60,
    ):
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.num_batches = num_batches
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout
        self.message_filtering = message_filtering
        self.delete_message_on_reception = delete_message_on_reception
        self.message_filtering_match_values = message_filtering_match_values
        self.message_filtering_config = message_filtering_config
        self.waiter_delay = waiter_delay

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "sqs_queue": self.sqs_queue,
                "aws_conn_id": self.aws_conn_id,
                "max_messages": self.max_messages,
                "num_batches": self.num_batches,
                "wait_time_seconds": self.wait_time_seconds,
                "visibility_timeout": self.visibility_timeout,
                "message_filtering": self.message_filtering,
                "delete_message_on_reception": self.delete_message_on_reception,
                "message_filtering_match_values": self.message_filtering_match_values,
                "message_filtering_config": self.message_filtering_config,
                "waiter_delay": self.waiter_delay,
            },
        )

    @property
    def hook(self) -> SqsHook:
        return SqsHook(aws_conn_id=self.aws_conn_id)

    async def poll_sqs(self, client: BaseAwsConnection) -> Collection:
        """
        Asynchronously poll SQS queue to retrieve messages.

        :param client: SQS connection
        :return: A list of messages retrieved from SQS
        """
        self.log.info("SqsSensor checking for message on queue: %s", self.sqs_queue)

        receive_message_kwargs = {
            "QueueUrl": self.sqs_queue,
            "MaxNumberOfMessages": self.max_messages,
            "WaitTimeSeconds": self.wait_time_seconds,
        }
        if self.visibility_timeout is not None:
            receive_message_kwargs["VisibilityTimeout"] = self.visibility_timeout

        response = await client.receive_message(**receive_message_kwargs)
        return response

    async def poke(self, client):
        message_batch: list[Any] = []

        for _ in range(self.num_batches):
            self.log.info("starting call to poll sqs")
            response = await self.poll_sqs(client=client)
            messages = process_response(
                response,
                self.message_filtering,
                self.message_filtering_match_values,
                self.message_filtering_config,
            )

            if not len(messages):
                continue

            message_batch.extend(messages)

            if self.delete_message_on_reception:

                self.log.info("Deleting %d messages", len(messages))

                entries = [
                    {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
                    for message in messages
                ]
                response = await client.delete_message_batch(QueueUrl=self.sqs_queue, Entries=entries)

                if "Successful" not in response:
                    raise AirflowException(
                        "Delete SQS Messages failed " + str(response) + " for messages " + str(messages)
                    )
        if not len(message_batch):
            return (False, None)
        return (True, message_batch)

    async def run(self):
        while True:
            # This loop will run indefinitely until the timeout, which is set in the self.defer
            # method, is reached.
            async with self.hook.async_conn as client:
                result = await self.poke(client=client)
                if result[0]:
                    yield TriggerEvent({"status": "success", "message_batch": result[1]})
                    break
                else:
                    await asyncio.sleep(self.waiter_delay)
