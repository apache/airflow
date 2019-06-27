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
#
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.slack_webhook_hook import SlackWebhookHook


class SlackWebhookOperator(SimpleHttpOperator):
    """
    This operator allows you to post messages to Slack using incoming webhooks.
    Takes both Slack webhook token directly and connection that has Slack webhook token.
    If both supplied, http_conn_id will be used as base_url,
    and webhook_token will be taken as endpoint, the relative path of the url.

    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.

    :param http_conn_id: connection that has Slack webhook token in the extra field
    :type http_conn_id: str
    :param webhook_token: Slack webhook token
    :type webhook_token: str
    :param message: The message you want to send on Slack
    :type message: str
    :param attachments: The attachments to send on Slack. Should be a list of
                        dictionaries representing Slack attachments.
    :type attachments: list
    :param channel: The channel the message should be posted to
    :type channel: str
    :param username: The username to post to slack with
    :type username: str
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :type icon_emoji: str
    :param link_names: Whether or not to find and link channel and usernames in your
                       message
    :type link_names: bool
    :param proxy: Proxy to use to make the Slack webhook call
    :type proxy: str
    """

    @apply_defaults
    def __init__(self,
                 http_conn_id=None,
                 webhook_token=None,
                 message="",
                 attachments=None,
                 channel=None,
                 username=None,
                 icon_emoji=None,
                 link_names=False,
                 proxy=None,
                 *args,
                 **kwargs):
        super().__init__(endpoint=webhook_token,
                         *args,
                         **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.attachments = attachments
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.link_names = link_names
        self.proxy = proxy
        self.hook = None

    def execute(self, context):
        """
        Call the SlackWebhookHook to post the provided Slack message
        """
        self.hook = SlackWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.attachments,
            self.channel,
            self.username,
            self.icon_emoji,
            self.link_names,
            self.proxy
        )
        self.hook.execute()
