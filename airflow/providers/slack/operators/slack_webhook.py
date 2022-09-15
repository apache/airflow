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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SlackWebhookOperator(SimpleHttpOperator):
    """
    This operator allows you to post messages to Slack using incoming webhooks.
    Takes both Slack webhook token directly and connection that has Slack webhook token.
    If both supplied, http_conn_id will be used as base_url,
    and webhook_token will be taken as endpoint, the relative path of the url.

    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.

    :param http_conn_id: connection that has Slack webhook token in the extra field
    :param webhook_token: Slack webhook token
    :param message: The message you want to send on Slack
    :param attachments: The attachments to send on Slack. Should be a list of
        dictionaries representing Slack attachments.
    :param blocks: The blocks to send on Slack. Should be a list of
        dictionaries representing Slack blocks.
    :param channel: The channel the message should be posted to
    :param username: The username to post to slack with
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :param icon_url: The icon image URL string to use in place of the default icon.
    :param link_names: Whether or not to find and link channel and usernames in your
        message
    :param proxy: Proxy to use to make the Slack webhook call
    """

    template_fields: Sequence[str] = (
        'webhook_token',
        'message',
        'attachments',
        'blocks',
        'channel',
        'username',
        'proxy',
    )

    def __init__(
        self,
        *,
        http_conn_id: str,
        webhook_token: str | None = None,
        message: str = "",
        attachments: list | None = None,
        blocks: list | None = None,
        channel: str | None = None,
        username: str | None = None,
        icon_emoji: str | None = None,
        icon_url: str | None = None,
        link_names: bool = False,
        proxy: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(endpoint=webhook_token, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.attachments = attachments
        self.blocks = blocks
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.icon_url = icon_url
        self.link_names = link_names
        self.proxy = proxy
        self.hook: SlackWebhookHook | None = None

    def execute(self, context: Context) -> None:
        """Call the SlackWebhookHook to post the provided Slack message"""
        self.hook = SlackWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.attachments,
            self.blocks,
            self.channel,
            self.username,
            self.icon_emoji,
            self.icon_url,
            self.link_names,
            self.proxy,
        )
        self.hook.execute()
