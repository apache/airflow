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

from collections.abc import Sequence
from typing import Any

from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger


class KafkaMessageQueueTrigger(MessageQueueTrigger):
    """
    A dedicated trigger for Kafka message queues that extends ``MessageQueueTrigger``.

    This trigger extends the common ``MessageQueueTrigger`` and is designed to work
    with the ``KafkaMessageQueueProvider``. It provides a more specific interface
    for Kafka message queue operations while leveraging the unified messaging framework.

    :param topics: The topic (or topic regex) that should be searched for messages
    :param kafka_config_id: The Kafka queue identifier in the format kafka://<broker>/<topic_list>
    :param apply_function: the location of the function to apply to messages for determination of matching
        criteria. (In python dot notation as a string)
    :param apply_function_args: A set of arguments to apply to the callable, defaults to None
    :param apply_function_kwargs: A set of key word arguments to apply to the callable, defaults to None,
        defaults to None
    :param poll_timeout: How long the Kafka client should wait before returning from a poll request to
        Kafka (seconds), defaults to 1
    :param poll_interval: How long the trigger should sleep after reaching the end of the Kafka log
        (seconds), defaults to 5

    """

    def __init__(
        self,
        *,
        topics: Sequence[str],
        kafka_config_id: str = "kafka_default",
        apply_function: str,
        apply_function_args: list[Any] | None = None,
        apply_function_kwargs: dict[Any, Any] | None = None,
        poll_timeout: float = 1,
        poll_interval: float = 5,
        **kwargs: Any,
    ) -> None:
        queue = self.__class__.get_kafka_queue_uri(kafka_config_id=kafka_config_id, topics=topics)
        # Pass all required parameters to MessageQueueTrigger
        super().__init__(
            queue=queue,
            apply_function=apply_function,
            kafka_config_id=kafka_config_id,
            apply_function_args=apply_function_args or [],
            apply_function_kwargs=apply_function_kwargs or {},
            poll_timeout=poll_timeout,
            poll_interval=poll_interval,
            **kwargs,
        )

    @classmethod
    def _get_brokers_from_connection(cls, kafka_config_id: str = "kafka_default") -> str:
        """
        Get the brokers from the Kafka connection.

        :param kafka_config_id: The Kafka connection ID, defaults to "kafka_default"
        :return: A string of brokers
        """
        from airflow.models.connection import Connection

        conn = Connection.get_connection_from_secrets(kafka_config_id)
        if not (brokers := conn.extra_dejson.get("bootstrap.servers", None)):
            raise ValueError("config['bootstrap.servers'] must be provided.")
        if isinstance(brokers, list):
            brokers = ",".join(brokers)
        return brokers.strip().replace(" ", "")

    @classmethod
    def get_kafka_queue_uri(cls, kafka_config_id: str, topics: Sequence[str]) -> str:
        """
        Generate a Kafka queue URI string from a Kafka configuration ID and a list of topics.

        :param kafka_config_id: The Kafka connection configuration ID.
        :param topics: A sequence of topic names to include in the URI.
        :return: A formatted Kafka URI string in the format "kafka://brokers/topics".
        """
        queue_topics = ",".join(topics).strip().replace(" ", "")
        if not queue_topics:
            raise ValueError("At least one valid topic must be provided.")
        queue_brokers = cls._get_brokers_from_connection(kafka_config_id)
        return f"kafka://{queue_brokers}/{queue_topics}"
