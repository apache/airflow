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
This module contains a Google PubSub sensor.
"""
import warnings
from typing import Optional

from google.protobuf.json_format import MessageToDict

from airflow.gcp.hooks.pubsub import PubSubHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class PubSubPullSensor(BaseSensorOperator):
    """Pulls messages from a PubSub subscription and passes them through XCom.

    This sensor operator will pull up to ``max_messages`` messages from the
    specified PubSub subscription. When the subscription returns messages,
    the poke method's criteria will be fulfilled and the messages will be
    returned from the operator and passed through XCom for downstream tasks.

    If ``ack_messages`` is set to True, messages will be immediately
    acknowledged before being returned, otherwise, downstream tasks will be
    responsible for acknowledging them.

    ``project`` and ``subscription`` are templated so you can use
    variables in them.

    :param project: the GCP project ID for the subscription (templated)
    :type project: str
    :param subscription: the Pub/Sub subscription name. Do not include the
        full subscription path.
    :type subscription: str
    :param max_messages: The maximum number of messages to retrieve per
        PubSub pull request
    :type max_messages: int
    :param return_immediately: If True, instruct the PubSub API to return
        immediately if no messages are available for delivery.
    :type return_immediately: bool
    :param ack_messages: If True, each message will be acknowledged
        immediately rather than by any downstream tasks
    :type ack_messages: bool
    :param gcp_conn_id: The connection ID to use connecting to
        Google Cloud Platform.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request
        must have domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ['project_id', 'subscription']
    ui_color = '#ff7f50'

    @apply_defaults
    def __init__(
            self,
            project_id: str,
            subscription: str,
            max_messages: int = 5,
            return_immediately: bool = False,
            ack_messages: bool = False,
            gcp_conn_id: str = 'google_cloud_default',
            delegate_to: Optional[str] = None,
            project: Optional[str] = None,
            *args,
            **kwargs) -> None:

        # To preserve backward compatibility
        # TODO: remove one day
        if project:
            warnings.warn(
                "The project parameter has been deprecated. You should pass "
                "the project_id parameter.", DeprecationWarning, stacklevel=2)
            project_id = project

        super().__init__(*args, **kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.project_id = project_id
        self.subscription = subscription
        self.max_messages = max_messages
        self.return_immediately = return_immediately
        self.ack_messages = ack_messages

        self._messages = None

    def execute(self, context):
        """Overridden to allow messages to be passed"""
        super().execute(context)
        return self._messages

    def poke(self, context):
        hook = PubSubHook(gcp_conn_id=self.gcp_conn_id,
                          delegate_to=self.delegate_to)
        pulled_messages = hook.pull(
            project_id=self.project_id,
            subscription=self.subscription,
            max_messages=self.max_messages,
            return_immediately=self.return_immediately
        )

        self._messages = [MessageToDict(m) for m in pulled_messages]

        if self._messages and self.ack_messages:
            ack_ids = [m['ackId'] for m in self._messages if m.get('ackId')]
            hook.acknowledge(project_id=self.project_id, subscription=self.subscription, ack_ids=ack_ids)
        return self._messages
