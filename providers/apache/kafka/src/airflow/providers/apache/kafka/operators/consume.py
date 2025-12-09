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

from collections.abc import Callable, Sequence
from functools import cached_property, partial
from typing import Any

from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import AirflowException, BaseOperator

VALID_COMMIT_CADENCE = {"never", "end_of_batch", "end_of_operator"}


class ConsumeFromTopicOperator(BaseOperator):
    """
    An operator that consumes from Kafka a topic(s) and processing the messages.

    The operator creates a Kafka consumer that reads a batch of messages from the cluster and processes them
    using the user supplied callable function. The consumer will continue to read in batches until it reaches
    the end of the log or reads a maximum number of messages is reached.

    :param kafka_config_id: The connection object to use, defaults to "kafka_default"
    :param topics: A list of topics or regex patterns the consumer should subscribe to.
    :param apply_function: The function that should be applied to fetched one at a time.
        name of dag file executing the function and the function name delimited by a `.`
    :param apply_function_batch: The function that should be applied to a batch of messages fetched. Can not
        be used with `apply_function`. Intended for transactional workloads where an expensive task might
        be called before or after operations on the messages are taken.
    :param apply_function_args: Additional arguments that should be applied to the callable, defaults to None
    :param apply_function_kwargs: Additional key word arguments that should be applied to the callable
        defaults to None
    :param commit_cadence: When consumers should commit offsets ("never", "end_of_batch","end_of_operator"),
        defaults to "end_of_operator";
        if end_of_operator, the commit() is called based on the max_messages arg. Commits are made after the
        operator has processed the apply_function method for the maximum messages in the operator.
        if end_of_batch, the commit() is called based on the max_batch_size arg. Commits are made after each
        batch has processed by the apply_function method for all messages in the batch.
        if never,  close() is called without calling the commit() method.
    :param max_messages: The maximum total number of messages an operator should read from Kafka,
        defaults to None implying read to the end of the topic.
    :param max_batch_size: The maximum number of messages a consumer should read when polling,
        defaults to 1000
    :param poll_timeout: How long the Kafka consumer should wait before determining no more messages are
        available, defaults to 60

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:ConsumeFromTopicOperator`
    """

    BLUE = "#ffefeb"
    ui_color = BLUE
    template_fields = (
        "topics",
        "apply_function_args",
        "apply_function_kwargs",
        "kafka_config_id",
    )

    def __init__(
        self,
        topics: str | Sequence[str],
        kafka_config_id: str = "kafka_default",
        apply_function: Callable[..., Any] | str | None = None,
        apply_function_batch: Callable[..., Any] | str | None = None,
        apply_function_args: Sequence[Any] | None = None,
        apply_function_kwargs: dict[Any, Any] | None = None,
        commit_cadence: str = "end_of_operator",
        max_messages: int | None = None,
        max_batch_size: int = 1000,
        poll_timeout: float = 60,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self.topics = topics
        self.apply_function = apply_function
        self.apply_function_batch = apply_function_batch
        self.apply_function_args = apply_function_args or ()
        self.apply_function_kwargs = apply_function_kwargs or {}
        self.kafka_config_id = kafka_config_id
        self.commit_cadence = commit_cadence
        self.max_messages = max_messages
        self.max_batch_size = max_batch_size
        self.poll_timeout = poll_timeout

        self.read_to_end = self.max_messages is None
        self._validate_commit_cadence_on_construct()

        if self.max_messages is not None and self.max_batch_size > self.max_messages:
            self.log.warning(
                "max_batch_size (%s) > max_messages (%s). Setting max_messages to %s ",
                self.max_batch_size,
                self.max_messages,
                self.max_batch_size,
            )
            self.max_messages = self.max_batch_size

        if apply_function and apply_function_batch:
            raise AirflowException(
                "One of apply_function or apply_function_batch must be supplied, not both."
            )

    @cached_property
    def hook(self):
        """Return the KafkaConsumerHook instance."""
        return KafkaConsumerHook(topics=self.topics, kafka_config_id=self.kafka_config_id)

    def execute(self, context) -> Any:
        self._validate_commit_cadence_before_execute()
        consumer = self.hook.get_consumer()

        if isinstance(self.apply_function, str):
            self.apply_function = import_string(self.apply_function)

        if isinstance(self.apply_function_batch, str):
            self.apply_function_batch = import_string(self.apply_function_batch)

        if self.apply_function is not None and not callable(self.apply_function):
            raise TypeError(f"apply_function is not a callable, got {type(self.apply_function)} instead.")

        if self.apply_function:
            apply_callable = partial(
                self.apply_function,
                *self.apply_function_args,
                **self.apply_function_kwargs,
            )

        if self.apply_function_batch is not None and not callable(self.apply_function_batch):
            raise TypeError(
                f"apply_function_batch is not a callable, got {type(self.apply_function_batch)} instead."
            )

        if self.apply_function_batch:
            apply_callable = partial(
                self.apply_function_batch,
                *self.apply_function_args,
                **self.apply_function_kwargs,
            )

        messages_left = self.max_messages or True

        while self.read_to_end or (
            messages_left > 0
        ):  # bool(True > 0) == True in the case where self.max_messages isn't set by the user
            if not isinstance(messages_left, bool):
                batch_size = self.max_batch_size if messages_left > self.max_batch_size else messages_left
            else:
                batch_size = self.max_batch_size

            msgs = consumer.consume(num_messages=batch_size, timeout=self.poll_timeout)
            if not self.read_to_end:
                messages_left -= len(msgs)

            if not msgs:  # No messages + messages_left is being used.
                self.log.info("Reached end of log. Exiting.")
                break

            if self.apply_function:
                for m in msgs:
                    apply_callable(m)

            if self.apply_function_batch:
                apply_callable(msgs)

            if self.commit_cadence == "end_of_batch":
                self.log.info("committing offset at %s", self.commit_cadence)
                consumer.commit()

        if self.commit_cadence != "never":
            self.log.info("committing offset at %s", self.commit_cadence)
            consumer.commit()

        consumer.close()

        return

    def _validate_commit_cadence_on_construct(self):
        """Validate the commit_cadence parameter when the operator is constructed."""
        if self.commit_cadence and self.commit_cadence not in VALID_COMMIT_CADENCE:
            raise AirflowException(
                f"commit_cadence must be one of {VALID_COMMIT_CADENCE}. Got {self.commit_cadence}"
            )

    def _validate_commit_cadence_before_execute(self):
        """Validate the commit_cadence parameter before executing the operator."""
        kafka_config = self.hook.get_connection(self.kafka_config_id).extra_dejson
        # Same as kafka's behavior, default to "true" if not set
        enable_auto_commit = str(kafka_config.get("enable.auto.commit", "true")).lower()

        if self.commit_cadence and enable_auto_commit != "false":
            self.log.warning(
                "To respect commit_cadence='%s', "
                "'enable.auto.commit' should be set to 'false' in the Kafka connection configuration. "
                "Currently, 'enable.auto.commit' is not explicitly set, so it defaults to 'true', which causes "
                "the consumer to auto-commit offsets every 5 seconds. "
                "See: https://kafka.apache.org/documentation/#consumerconfigs_enable.auto.commit for more information",
                self.commit_cadence,
            )
