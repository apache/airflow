#
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
"""Reads and then deletes the message from SQS queue."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Collection, Sequence

from deprecated import deprecated
from typing_extensions import Literal

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.providers.amazon.aws.hooks.sqs import SqsHook
from airflow.providers.amazon.aws.triggers.sqs import SqsSensorTrigger
from airflow.providers.amazon.aws.utils.sqs import process_response
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection
    from airflow.utils.context import Context
from datetime import timedelta


class SqsSensor(BaseSensorOperator):
    """
    Get messages from an Amazon SQS queue and then delete the messages from the queue.

    If deletion of messages fails, an AirflowException is thrown. Otherwise, the messages
    are pushed through XCom with the key ``messages``.

    By default,the sensor performs one and only one SQS call per poke, which limits the result to
    a maximum of 10 messages. However, the total number of SQS API calls per poke can be controlled
    by num_batches param.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:SqsSensor`

    :param aws_conn_id: AWS connection id
    :param sqs_queue: The SQS queue url (templated)
    :param max_messages: The maximum number of messages to retrieve for each poke (templated)
    :param num_batches: The number of times the sensor will call the SQS API to receive messages (default: 1)
    :param wait_time_seconds: The time in seconds to wait for receiving messages (default: 1 second)
    :param visibility_timeout: Visibility timeout, a period of time during which
        Amazon SQS prevents other consumers from receiving and processing the message.
    :param message_filtering: Specified how received messages should be filtered. Supported options are:
        `None` (no filtering, default), `'literal'` (message Body literal match) or `'jsonpath'`
        (message Body filtered using a JSONPath expression).
        You may add further methods by overriding the relevant class methods.
    :param message_filtering_match_values: Optional value/s for the message filter to match on.
        For example, with literal matching, if a message body matches any of the specified values
        then it is included. For JSONPath matching, the result of the JSONPath expression is used
        and may match any of the specified values.
    :param message_filtering_config: Additional configuration to pass to the message filter.
        For example with JSONPath filtering you can pass a JSONPath expression string here,
        such as `'foo[*].baz'`. Messages with a Body which does not match are ignored.
    :param delete_message_on_reception: Default to `True`, the messages are deleted from the queue
        as soon as being consumed. Otherwise, the messages remain in the queue after consumption and
        should be deleted manually.
    :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)

    """

    template_fields: Sequence[str] = ("sqs_queue", "max_messages", "message_filtering_config")

    def __init__(
        self,
        *,
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
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.num_batches = num_batches
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout

        self.message_filtering = message_filtering

        self.delete_message_on_reception = delete_message_on_reception

        if message_filtering_match_values is not None:
            if not isinstance(message_filtering_match_values, set):
                message_filtering_match_values = set(message_filtering_match_values)
        self.message_filtering_match_values = message_filtering_match_values

        if self.message_filtering == "literal":
            if self.message_filtering_match_values is None:
                raise TypeError("message_filtering_match_values must be specified for literal matching")

        self.message_filtering_config = message_filtering_config
        self.deferrable = deferrable

    def execute(self, context: Context) -> Any:
        if self.deferrable:
            self.defer(
                trigger=SqsSensorTrigger(
                    sqs_queue=self.sqs_queue,
                    aws_conn_id=self.aws_conn_id,
                    max_messages=self.max_messages,
                    num_batches=self.num_batches,
                    wait_time_seconds=self.wait_time_seconds,
                    visibility_timeout=self.visibility_timeout,
                    message_filtering=self.message_filtering,
                    message_filtering_match_values=self.message_filtering_match_values,
                    message_filtering_config=self.message_filtering_config,
                    delete_message_on_reception=self.delete_message_on_reception,
                    waiter_delay=int(self.poke_interval),
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.timeout),
            )
        else:
            super().execute(context=context)

    def execute_complete(self, context: Context, event: dict | None = None) -> None:
        if event is None or event["status"] != "success":
            # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
            message = f"Trigger error: event is {event}"
            if self.soft_fail:
                raise AirflowSkipException(message)
            raise AirflowException(message)
        context["ti"].xcom_push(key="messages", value=event["message_batch"])

    def poll_sqs(self, sqs_conn: BaseAwsConnection) -> Collection:
        """
        Poll SQS queue to retrieve messages.

        :param sqs_conn: SQS connection
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

        response = sqs_conn.receive_message(**receive_message_kwargs)
        return response

    def poke(self, context: Context):
        """
        Check subscribed queue for messages and write them to xcom with the ``messages`` key.

        :param context: the context object
        :return: ``True`` if message is available or ``False``
        """
        message_batch: list[Any] = []

        # perform multiple SQS call to retrieve messages in series
        for _ in range(self.num_batches):
            response = self.poll_sqs(sqs_conn=self.hook.conn)
            messages = process_response(
                response,
                self.message_filtering,
                self.message_filtering_match_values,
                self.message_filtering_config,
            )

            if not messages:
                continue

            message_batch.extend(messages)

            if self.delete_message_on_reception:

                self.log.info("Deleting %d messages", len(messages))

                entries = [
                    {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
                    for message in messages
                ]
                response = self.hook.conn.delete_message_batch(QueueUrl=self.sqs_queue, Entries=entries)

                if "Successful" not in response:
                    # TODO: remove this if block when min_airflow_version is set to higher than 2.7.1
                    error_message = f"Delete SQS Messages failed {response} for messages {messages}"
                    if self.soft_fail:
                        raise AirflowSkipException(error_message)
                    raise AirflowException(error_message)
        if message_batch:
            context["ti"].xcom_push(key="messages", value=message_batch)
            return True
        else:
            return False

    @deprecated(reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning)
    def get_hook(self) -> SqsHook:
        """Create and return an SqsHook."""
        return self.hook

    @cached_property
    def hook(self) -> SqsHook:
        return SqsHook(aws_conn_id=self.aws_conn_id)
