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

import json
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.slack.hooks.slack import SlackHook

try:
    from airflow.notifications.basenotifier import BaseNotifier
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "Failed to import BaseNotifier. This feature is only available in Airflow versions >= 2.6.0"
    )

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler

ICON_URL: str = "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static/pin_100.png"


class SlackNotifier(BaseNotifier):
    """
    Slack BaseNotifier.

    :param slack_conn_id: Slack API token (https://api.slack.com/web).
    :param text: The content of the message
    :param channel: The channel to send the message to. Optional
    :param username: The username to send the message as. Optional
    :param icon_url: The icon to use for the message. Optional
    :param attachments: A list of attachments to send with the message. Optional
    :param blocks: A list of blocks to send with the message. Optional
    :param timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. Optional
    :param base_url: A string representing the Slack API base URL. Optional
    :param proxy: Proxy to make the Slack API call. Optional
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebClient``. Optional
    """

    template_fields = ("text", "channel", "username", "attachments", "blocks")

    def __init__(
        self,
        *,
        slack_conn_id: str = SlackHook.default_conn_name,
        text: str = "This is a default message",
        channel: str = "#general",
        username: str = "Airflow",
        icon_url: str = ICON_URL,
        attachments: Sequence = (),
        blocks: Sequence = (),
        base_url: str | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        retry_handlers: list[RetryHandler] | None = None,
    ):
        super().__init__()
        self.slack_conn_id = slack_conn_id
        self.text = text
        self.channel = channel
        self.username = username
        self.icon_url = icon_url
        self.attachments = attachments
        self.blocks = blocks
        self.base_url = base_url
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers

    @cached_property
    def hook(self) -> SlackHook:
        """Slack Hook."""
        return SlackHook(
            slack_conn_id=self.slack_conn_id,
            base_url=self.base_url,
            timeout=self.timeout,
            proxy=self.proxy,
            retry_handlers=self.retry_handlers,
        )

    def notify(self, context):
        """Send a message to a Slack Channel."""
        api_call_params = {
            "channel": self.channel,
            "username": self.username,
            "text": self.text,
            "icon_url": self.icon_url,
            "attachments": json.dumps(self.attachments),
            "blocks": json.dumps(self.blocks),
        }
        self.hook.call("chat.postMessage", json=api_call_params)


send_slack_notification = SlackNotifier
