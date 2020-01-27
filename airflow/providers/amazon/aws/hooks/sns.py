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

"""
This module contains AWS SNS hook
"""
import json

from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook


class AwsSnsHook(AwsHook):
    """
    Interact with Amazon Simple Notification Service.
    """

    def __init__(self, *args, **kwargs):
        self.conn = None
        super().__init__(*args, **kwargs)

    def get_conn(self):
        """
        Get an SNS connection
        """
        self.conn = self.get_client_type('sns')
        return self.conn

    def publish_to_target(self, target_arn, message, subject=None):
        """
        Publish a message to a topic or an endpoint.

        :param target_arn: either a TopicArn or an EndpointArn
        :type target_arn: str
        :param message: the default message you want to send
        :param message: str
        :param subject: subject of message
        :type subject: str
        """

        conn = self.get_conn()

        messages = {
            'default': message
        }

        if subject is None:
            return conn.publish(
                TargetArn=target_arn,
                Message=json.dumps(messages),
                MessageStructure='json'
            )

        return conn.publish(
            TargetArn=target_arn,
            Message=json.dumps(messages),
            MessageStructure='json',
            Subject=subject
        )
