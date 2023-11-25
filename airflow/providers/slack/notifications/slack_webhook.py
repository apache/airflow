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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

try:
    from airflow.notifications.basenotifier import BaseNotifier
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import BaseNotifier. This feature is only available in Airflow versions >= 2.6.0"
    )

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler


class SlackWebhookNotifier(BaseNotifier):
    """
    Slack Incoming Webhooks Notifier.

    .. note::
        ``SlackWebhookNotifier`` provide integration with Slack Incoming Webhooks,
        and may not function accurately within Legacy Slack Integration Incoming Webhook.

    :param slack_webhook_conn_id: :ref:`Slack Incoming Webhook <howto/connection:slack>`
        connection id that has Incoming Webhook token in the password field.
    :param text: The content of the message
    :param blocks: A list of blocks to send with the message. Optional
    :param unfurl_links: Option to indicate whether text url should unfurl. Optional
    :param unfurl_media: Option to indicate whether media url should unfurl. Optional
    :param timeout: The maximum number of seconds the client will wait to connect. Optional
        and receive a response from Slack. Optional
    :param proxy: Proxy to make the Slack Incoming Webhook call. Optional
    :param attachments: (legacy) A list of attachments to send with the message. Optional
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebhookClient``. Optional
    """

    template_fields = ("slack_webhook_conn_id", "text", "attachments", "blocks", "proxy", "timeout")

    def __init__(
        self,
        *,
        slack_webhook_conn_id: str = SlackWebhookHook.default_conn_name,
        text: str,
        blocks: list | None = None,
        unfurl_links: bool | None = None,
        unfurl_media: bool | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        attachments: list | None = None,
        retry_handlers: list[RetryHandler] | None = None,
    ):
        super().__init__()
        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.text = text
        self.attachments = attachments
        self.blocks = blocks
        self.unfurl_links = unfurl_links
        self.unfurl_media = unfurl_media
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers

    @cached_property
    def hook(self) -> SlackWebhookHook:
        """Slack Incoming Webhook Hook."""
        return SlackWebhookHook(
            slack_webhook_conn_id=self.slack_webhook_conn_id,
            proxy=self.proxy,
            timeout=self.timeout,
            retry_handlers=self.retry_handlers,
        )

    def notify(self, context):
        """Send a message to a Slack Incoming Webhook."""
        self.hook.send(
            text=self.text,
            blocks=self.blocks,
            unfurl_links=self.unfurl_links,
            unfurl_media=self.unfurl_media,
            attachments=self.attachments,
        )


send_slack_webhook_notification = SlackWebhookNotifier
