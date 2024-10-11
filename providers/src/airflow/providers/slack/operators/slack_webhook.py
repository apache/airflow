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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler

    from airflow.utils.context import Context


class SlackWebhookOperator(BaseOperator):
    """
    This operator allows you to post messages to Slack using Incoming Webhooks.

    .. note::
        You cannot override the default channel (chosen by the user who installed your app),
        username, or icon when you're using Incoming Webhooks to post messages.
        Instead, these values will always inherit from the associated Slack App configuration
        (`link <https://api.slack.com/messaging/webhooks#advanced_message_formatting>`_).
        It is possible to change this values only in `Legacy Slack Integration Incoming Webhook
        <https://api.slack.com/legacy/custom-integrations/messaging/webhooks#legacy-customizations>`_.

    :param slack_webhook_conn_id: :ref:`Slack Incoming Webhook <howto/connection:slack>`
        connection id that has Incoming Webhook token in the password field.
    :param message: The formatted text of the message to be published.
        If ``blocks`` are included, this will become the fallback text used in notifications.
    :param attachments: The attachments to send on Slack. Should be a list of
        dictionaries representing Slack attachments.
    :param blocks: The blocks to send on Slack. Should be a list of
        dictionaries representing Slack blocks.
    :param channel: The channel the message should be posted to
    :param username: The username to post to slack with
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :param icon_url: The icon image URL string to use in place of the default icon.
    :param proxy: Proxy to make the Slack Incoming Webhook call. Optional
    :param timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. Optional
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebhookClient``. Optional
    """

    template_fields: Sequence[str] = (
        "message",
        "attachments",
        "blocks",
        "channel",
        "username",
        "proxy",
    )

    def __init__(
        self,
        *,
        slack_webhook_conn_id,
        message: str = "",
        attachments: list | None = None,
        blocks: list | None = None,
        channel: str | None = None,
        username: str | None = None,
        icon_emoji: str | None = None,
        icon_url: str | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.proxy = proxy
        self.message = message
        self.attachments = attachments
        self.blocks = blocks
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.icon_url = icon_url
        self.timeout = timeout
        self.retry_handlers = retry_handlers

    @cached_property
    def hook(self) -> SlackWebhookHook:
        """Create and return an SlackWebhookHook (cached)."""
        return SlackWebhookHook(
            slack_webhook_conn_id=self.slack_webhook_conn_id,
            proxy=self.proxy,
            timeout=self.timeout,
            retry_handlers=self.retry_handlers,
        )

    def execute(self, context: Context) -> None:
        """Call the SlackWebhookHook to post the provided Slack message."""
        self.hook.send(
            text=self.message,
            attachments=self.attachments,
            blocks=self.blocks,
            # Parameters below use for compatibility with previous version of Operator and warn user if it set
            # Legacy Integration Parameters
            channel=self.channel,
            username=self.username,
            icon_emoji=self.icon_emoji,
            icon_url=self.icon_url,
        )
