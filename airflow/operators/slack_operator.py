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

import json
from typing import Dict, List, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.decorators import apply_defaults


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well

    :param slack_conn_id: Slack connection ID which its password is Slack API token
    :type slack_conn_id: str
    :param token: Slack API token (https://api.slack.com/web)
    :type token: str
    :param method: The Slack API Method to Call (https://api.slack.com/methods)
    :type method: str
    :param api_params: API Method call parameters (https://api.slack.com/methods)
    :type api_params: dict
    """

    @apply_defaults
    def __init__(self,
                 slack_conn_id: Optional[str] = None,
                 token: Optional[str] = None,
                 method: Optional[str] = None,
                 api_params: Optional[Dict] = None,
                 *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if token is None and slack_conn_id is None:
            raise AirflowException('No valid Slack token nor slack_conn_id supplied.')
        if token is not None and slack_conn_id is not None:
            raise AirflowException('Cannot determine Slack credential '
                                   'when both token and slack_conn_id are supplied.')

        self.token = token  # type: Optional[str]
        self.slack_conn_id = slack_conn_id  # type: Optional[str]

        self.method = method
        self.api_params = api_params

    def construct_api_call_params(self):
        """
        Used by the execute function. Allows templating on the source fields
        of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for
        having a construct_api_call_params function
        which sets self.api_call_params with a dict of
        API call parameters (https://api.slack.com/methods)
        """

    def execute(self, **kwargs):
        """
        SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()
        slack = SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)
        slack.call(self.method, self.api_params)


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel

    :param channel: channel in which to post message on slack name (#general) or
        ID (C12318391). (templated)
    :type channel: str
    :param username: Username that airflow will be posting to Slack as. (templated)
    :type username: str
    :param text: message to send to slack. (templated)
    :type text: str
    :param icon_url: url to icon used for this message
    :type icon_url: str
    :param attachments: extra formatting details. (templated)
        - see https://api.slack.com/docs/attachments.
    :type attachments: list of hashes
    :param blocks: extra block layouts. (templated)
        - see https://api.slack.com/reference/block-kit/blocks.
    :type blocks: list of hashes
    """

    template_fields = ('username', 'text', 'attachments', 'blocks', 'channel')
    ui_color = '#FFBA40'

    @apply_defaults
    def __init__(self,
                 channel: str = '#general',
                 username: str = 'Airflow',
                 text: str = 'No message has been set.\n'
                             'Here is a cat video instead\n'
                             'https://www.youtube.com/watch?v=J---aiyznGQ',
                 icon_url: str = 'https://raw.githubusercontent.com/apache/'
                                 'airflow/master/airflow/www/static/pin_100.png',
                 attachments: Optional[List] = None,
                 blocks: Optional[List] = None,
                 *args, **kwargs):
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []
        super().__init__(method=self.method,
                         *args, **kwargs)

    def construct_api_call_params(self):
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
            'blocks': json.dumps(self.blocks),
        }
