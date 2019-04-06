# -*- coding: utf-8 -*-
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

from airflow.contrib.hooks.aws_hook import AwsHook


class SQSHook(AwsHook):
    """
    Get the SQS client using boto3 library
    :return: SQS client
    :rtype: botocore.client.SQS
    """

    def get_conn(self):
        return self.get_client_type('sqs')

    """
    Create queue using connection object
    :param name: name of the queue.
    :type name: str
    :param attributes: additional attributes for the queue
    :type attributes: dict

        For details of the attributes parameter see :py:meth:`botocore.client.SQS.create_queue`

    :return: dict with the information about the queue
    :rtype: dict

        For details of the returned value see :py:meth:`botocore.client.SQS.create_queue`
    """
    def create_queue(self, queue_name, attributes={}):
        return self.get_conn().create_queue(QueueName=queue_name, Attributes=attributes)

    """
    Send message to the queue
    :param queue_url: queue url
    :type name: str
    :param message_body: the contents of the message
    :type name: str
    :param delay_seconds: seconds to delay the message
    :type name: int
    :param message_attributes: additional attributes for the message
    :type attributes: dict

        For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`

    :return: dict with the information about the message sent
    :rtype: dict

        For details of the returned value see :py:meth:`botocore.client.SQS.send_message`
    """
    def send_message(self, queue_url, message_body, delay_seconds=0, message_attributes={}):
        return self.get_conn().send_message(QueueUrl=queue_url,
                                            MessageBody=message_body,
                                            DelaySeconds=delay_seconds,
                                            MessageAttributes=message_attributes)
