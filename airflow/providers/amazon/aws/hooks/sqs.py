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

"""This module contains AWS SQS hook"""
import warnings
from typing import Dict, Optional

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SqsHook(AwsBaseHook):
    """
    Interact with Amazon Simple Queue Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "sqs"
        super().__init__(*args, **kwargs)

    def create_queue(self, queue_name: str, attributes: Optional[Dict] = None) -> Dict:
        """
        Create queue using connection object

        :param queue_name: name of the queue.
        :param attributes: additional attributes for the queue (default: None)
            For details of the attributes parameter see :py:meth:`SQS.create_queue`

        :return: dict with the information about the queue
            For details of the returned value see :py:meth:`SQS.create_queue`
        :rtype: dict
        """
        return self.get_conn().create_queue(QueueName=queue_name, Attributes=attributes or {})

    def send_message(
        self,
        queue_url: str,
        message_body: str,
        delay_seconds: int = 0,
        message_attributes: Optional[Dict] = None,
        message_group_id: Optional[str] = None,
    ) -> Dict:
        """
        Send message to the queue

        :param queue_url: queue url
        :param message_body: the contents of the message
        :param delay_seconds: seconds to delay the message
        :param message_attributes: additional attributes for the message (default: None)
            For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
        :param message_group_id: This applies only to FIFO (first-in-first-out) queues. (default: None)
            For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`

        :return: dict with the information about the message sent
            For details of the returned value see :py:meth:`botocore.client.SQS.send_message`
        :rtype: dict
        """
        params = {
            'QueueUrl': queue_url,
            'MessageBody': message_body,
            'DelaySeconds': delay_seconds,
            'MessageAttributes': message_attributes or {},
        }
        if message_group_id:
            params['MessageGroupId'] = message_group_id

        return self.get_conn().send_message(**params)


class SQSHook(SqsHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.sqs.SqsHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. Please use :class:`airflow.providers.amazon.aws.hooks.sqs.SqsHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
