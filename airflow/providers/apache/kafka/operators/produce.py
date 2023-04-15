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

import logging
from functools import partial
from typing import Any, Callable, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.apache.kafka.hooks.produce import KafkaProducerHook
from airflow.utils.module_loading import import_string

local_logger = logging.getLogger("airflow")


def acked(err, msg):
    if err is not None:
        local_logger.error(f"Failed to deliver message: {err}")
    else:
        local_logger.info(
            f"Produced record to topic {msg.topic()} partition [{msg.partition()}] @ offset {msg.offset()}"
        )


class ProduceToTopicOperator(BaseOperator):
    """An operator that produces messages to a Kafka topic

    Registers a producer to a kafka topic and publishes messages to the log.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    :param topic: The topic the producer should produce to, defaults to None
    :param producer_function: The function that generates key/value pairs as messages for production,
        defaults to None
    :param producer_function_args: Additional arguments to be applied to the producer callable,
        defaults to None
    :param producer_function_kwargs: Additional keyword arguments to be applied to the producer callable,
        defaults to None
    :param delivery_callback: The callback to apply after delivery(or failure) of a message, defaults to None
    :param synchronous: If writes to kafka should be fully synchronous, defaults to True
    :param poll_timeout: How long of a delay should be applied when calling poll after production to kafka,
        defaults to 0
    :raises AirflowException: _description_

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ProduceToTopicOperator`
    """

    template_fields = (
        "topic",
        "producer_function",
        "producer_function_args",
        "producer_function_kwargs",
        "kafka_config_id",
    )

    def __init__(
        self,
        topic: str,
        producer_function: str | Callable[..., Any],
        kafka_config_id: str = "kafka_default",
        producer_function_args: Sequence[Any] | None = None,
        producer_function_kwargs: dict[Any, Any] | None = None,
        delivery_callback: str | None = None,
        synchronous: bool = True,
        poll_timeout: float = 0,
        **kwargs: Any,
    ) -> None:

        super().__init__(**kwargs)

        if delivery_callback:
            dc = import_string(delivery_callback)
        else:
            dc = acked

        self.kafka_config_id = kafka_config_id
        self.topic = topic
        self.producer_function = producer_function
        self.producer_function_args = producer_function_args or ()
        self.producer_function_kwargs = producer_function_kwargs or {}
        self.delivery_callback = dc
        self.synchronous = synchronous
        self.poll_timeout = poll_timeout

        if not (self.topic and self.producer_function):
            raise AirflowException(
                "topic and producer_function must be provided. Got topic="
                f"{self.topic} and producer_function={self.producer_function}"
            )

        return

    def execute(self, context) -> None:

        # Get producer and callable
        producer = KafkaProducerHook(kafka_config_id=self.kafka_config_id).get_producer()

        if isinstance(self.producer_function, str):
            self.producer_function = import_string(self.producer_function)

        producer_callable = partial(
            self.producer_function,  # type: ignore
            *self.producer_function_args,
            **self.producer_function_kwargs,
        )

        # For each returned k/v in the callable : publish and flush if needed.
        for k, v in producer_callable():
            producer.produce(self.topic, key=k, value=v, on_delivery=self.delivery_callback)
            producer.poll(self.poll_timeout)
            if self.synchronous:
                producer.flush()

        producer.flush()
