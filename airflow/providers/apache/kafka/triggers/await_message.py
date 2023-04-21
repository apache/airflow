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
from functools import partial
from typing import Any, Sequence

from asgiref.sync import sync_to_async

from airflow import AirflowException
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string


class AwaitMessageTrigger(BaseTrigger):
    """A trigger that waits for a message matching specific criteria to arrive in Kafka

    The behavior of the consumer of this trigger is as follows:
    - poll the Kafka topics for a message, if no message returned, sleep
    - process the message with provided callable and commit the message offset:

        - if callable returns any data, raise a TriggerEvent with the return data

        - else continue to next message


    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    :param topics: The topic (or topic regex) that should be searched for messages
    :param apply_function: the location of the function to apply to messages for determination of matching
        criteria. (In python dot notation as a string)
    :param apply_function_args: A set of arguments to apply to the callable, defaults to None
    :param apply_function_kwargs: A set of key word arguments to apply to the callable, defaults to None,
        defaults to None
    :param poll_timeout: How long the Kafka client should wait before returning from a poll request to
        Kafka (seconds), defaults to 1
    :param poll_interval: How long the the trigger should sleep after reaching the end of the Kafka log
        (seconds), defaults to 5

    """

    def __init__(
        self,
        topics: Sequence[str],
        apply_function: str,
        kafka_config_id: str = "kafka_default",
        apply_function_args: Sequence[Any] | None = None,
        apply_function_kwargs: dict[Any, Any] | None = None,
        poll_timeout: float = 1,
        poll_interval: float = 5,
    ) -> None:

        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}
        self.kafka_config_id = kafka_config_id
        self.poll_timeout = poll_timeout
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.apache.kafka.triggers.await_message.AwaitMessageTrigger",
            {
                "topics": self.topics,
                "apply_function": self.apply_function,
                "apply_function_args": self.apply_function_args,
                "apply_function_kwargs": self.apply_function_kwargs,
                "kafka_config_id": self.kafka_config_id,
                "poll_timeout": self.poll_timeout,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self):
        consumer_hook = KafkaConsumerHook(topics=self.topics, kafka_config_id=self.kafka_config_id)

        async_get_consumer = sync_to_async(consumer_hook.get_consumer)
        consumer = await async_get_consumer()

        async_poll = sync_to_async(consumer.poll)
        async_commit = sync_to_async(consumer.commit)

        processing_call = import_string(self.apply_function)
        processing_call = partial(processing_call, *self.apply_function_args, **self.apply_function_kwargs)
        async_message_process = sync_to_async(processing_call)
        while True:

            message = await async_poll(self.poll_timeout)

            if message is None:
                continue
            elif message.error():
                raise AirflowException(f"Error: {message.error()}")
            else:

                rv = await async_message_process(message)
                if rv:
                    await async_commit(asynchronous=False)
                    yield TriggerEvent(rv)
                else:
                    await async_commit(asynchronous=False)
                    await asyncio.sleep(self.poll_interval)
