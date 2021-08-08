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
"""Reads and then deletes the message from SQS queue"""
import json
from typing import Any, Optional

from jsonpath_ng import parse
from typing_extensions import Literal

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sqs import SQSHook
from airflow.sensors.base import BaseSensorOperator


class SQSSensor(BaseSensorOperator):
    """
    Get messages from an SQS queue and then deletes  the message from the SQS queue.
    If deletion of messages fails an AirflowException is thrown otherwise, the message
    is pushed through XCom with the key ``messages``.

    :param aws_conn_id: AWS connection id
    :type aws_conn_id: str
    :param sqs_queue: The SQS queue url (templated)
    :type sqs_queue: str
    :param max_messages: The maximum number of messages to retrieve for each poke (templated)
    :type max_messages: int
    :param wait_time_seconds: The time in seconds to wait for receiving messages (default: 1 second)
    :type wait_time_seconds: int
    :param visibility_timeout: Visibility timeout, a period of time during which
        Amazon SQS prevents other consumers from receiving and processing the message.
    :type visibility_timeout: Optional[Int]
    :param message_filtering: Specified how received messages should be filtered. Supported options are:
        `None` (no filtering, default), `'literal'` (message Body literal match) or `'jsonpath'`
        (message Body filtered using a JSONPath expression).
        You may add further methods by overriding the relevant class methods.
    :type message_filtering: Optional[Literal["literal", "jsonpath"]]
    :param message_filtering_match_values: Optional value/s for the message filter to match on.
        For example, with literal matching, if a message body matches any of the specified values
        then it is included. For JSONPath matching, the result of the JSONPath expression is used
        and may match any of the specified values.
    :type message_filtering_match_values: Any
    :param message_filtering_config: Additional configuration to pass to the message filter.
        For example with JSONPath filtering you can pass a JSONPath expression string here,
        such as `'foo[*].baz'`. Messages with a Body which does not match are ignored.
    :type message_filtering_config: Any
    """

    template_fields = ('sqs_queue', 'max_messages', 'message_filtering_config')

    def __init__(
        self,
        *,
        sqs_queue,
        aws_conn_id: str = 'aws_default',
        max_messages: int = 5,
        wait_time_seconds: int = 1,
        visibility_timeout: Optional[int] = None,
        message_filtering: Optional[Literal["literal", "jsonpath"]] = None,
        message_filtering_match_values: Any = None,
        message_filtering_config: Any = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sqs_queue = sqs_queue
        self.aws_conn_id = aws_conn_id
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout

        self.message_filtering = message_filtering

        if message_filtering_match_values is not None:
            if not isinstance(message_filtering_match_values, set):
                message_filtering_match_values = set(message_filtering_match_values)
        self.message_filtering_match_values = message_filtering_match_values

        if self.message_filtering == 'literal':
            if self.message_filtering_match_values is None:
                raise TypeError('message_filtering_match_values must be specified for literal matching')

        self.message_filtering_config = message_filtering_config

        self.hook: Optional[SQSHook] = None

    def poke(self, context):
        """
        Check for message on subscribed queue and write to xcom the message with key ``messages``

        :param context: the context object
        :type context: dict
        :return: ``True`` if message is available or ``False``
        """
        sqs_conn = self.get_hook().get_conn()

        self.log.info('SQSSensor checking for message on queue: %s', self.sqs_queue)

        receive_message_kwargs = {
            'QueueUrl': self.sqs_queue,
            'MaxNumberOfMessages': self.max_messages,
            'WaitTimeSeconds': self.wait_time_seconds,
        }
        if self.visibility_timeout is not None:
            receive_message_kwargs['VisibilityTimeout'] = self.visibility_timeout

        response = sqs_conn.receive_message(**receive_message_kwargs)

        if "Messages" not in response:
            return False

        messages = response['Messages']
        num_messages = len(messages)
        self.log.info("Received %d messages", num_messages)

        if not num_messages:
            return False

        if self.message_filtering:
            messages = self.filter_messages(messages)
            num_messages = len(messages)
            self.log.info("There are %d messages left after filtering", num_messages)

        if not num_messages:
            return False

        self.log.info("Deleting %d messages", num_messages)

        entries = [
            {'Id': message['MessageId'], 'ReceiptHandle': message['ReceiptHandle']} for message in messages
        ]
        response = sqs_conn.delete_message_batch(QueueUrl=self.sqs_queue, Entries=entries)

        if 'Successful' in response:
            context['ti'].xcom_push(key='messages', value=messages)
            return True
        else:
            raise AirflowException(
                'Delete SQS Messages failed ' + str(response) + ' for messages ' + str(messages)
            )

    def get_hook(self) -> SQSHook:
        """Create and return an SQSHook"""
        if self.hook:
            return self.hook

        self.hook = SQSHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def filter_messages(self, messages):
        if self.message_filtering == 'literal':
            return self.filter_messages_literal(messages)
        if self.message_filtering == 'jsonpath':
            return self.filter_messages_jsonpath(messages)
        else:
            raise NotImplementedError('Override this method to define custom filters')

    def filter_messages_literal(self, messages):
        filtered_messages = []
        for message in messages:
            if message['Body'] in self.message_filtering_match_values:
                filtered_messages.append(message)
        return filtered_messages

    def filter_messages_jsonpath(self, messages):
        jsonpath_expr = parse(self.message_filtering_config)
        filtered_messages = []
        for message in messages:
            body = message['Body']
            # Body is a string, deserialise to an object and then parse
            body = json.loads(body)
            results = jsonpath_expr.find(body)
            if not results:
                continue
            if self.message_filtering_match_values is None:
                filtered_messages.append(message)
                continue
            for result in results:
                if result.value in self.message_filtering_match_values:
                    filtered_messages.append(message)
                    break
        return filtered_messages
