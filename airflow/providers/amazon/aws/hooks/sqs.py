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

"""
This module contains AWS SQS hook
"""
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class SQSHook(AwsBaseHook):
    """
    Interact with Amazon Simple Queue Service.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs):
        super().__init__(client_type='sqs', *args, **kwargs)

    def create_queue(self, queue_name, attributes=None):
        """
        Create queue using connection object

        :param queue_name: name of the queue.
        :type queue_name: str
        :param attributes: additional attributes for the queue (default: None)
            For details of the attributes parameter see :py:meth:`botocore.client.SQS.create_queue`
        :type attributes: dict

        :return: dict with the information about the queue
            For details of the returned value see :py:meth:`botocore.client.SQS.create_queue`
        :rtype: dict
        """
        return self.get_conn().create_queue(QueueName=queue_name, Attributes=attributes or {})

    def send_message(self, queue_url, message_body, delay_seconds=0, message_attributes=None):
        """
        Send message to the queue

        :param queue_url: queue url
        :type queue_url: str
        :param message_body: the contents of the message
        :type message_body: str
        :param delay_seconds: seconds to delay the message
        :type delay_seconds: int
        :param message_attributes: additional attributes for the message (default: None)
            For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
        :type message_attributes: dict

        :return: dict with the information about the message sent
            For details of the returned value see :py:meth:`botocore.client.SQS.send_message`
        :rtype: dict
        """
        return self.get_conn().send_message(QueueUrl=queue_url,
                                            MessageBody=message_body,
                                            DelaySeconds=delay_seconds,
                                            MessageAttributes=message_attributes or {})
