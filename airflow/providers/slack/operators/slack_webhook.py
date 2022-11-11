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

import warnings
from typing import TYPE_CHECKING, Sequence

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

if TYPE_CHECKING:
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

    .. warning::
        This operator could take Slack Webhook Token from ``webhook_token``
        as well as from :ref:`Slack Incoming Webhook connection <howto/connection:slack-incoming-webhook>`.
        However, provide ``webhook_token`` it is not secure and this attribute
        will be removed in the future version of provider.

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
    :param link_names: Whether or not to find and link channel and usernames in your
        message
    :param proxy: Proxy to use to make the Slack webhook call
    :param webhook_token: (deprecated) Slack Incoming Webhook token.
        Please use ``slack_webhook_conn_id`` instead.
    """

    template_fields: Sequence[str] = (
        "webhook_token",
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
        slack_webhook_conn_id: str | None = None,
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
        http_conn_id = kwargs.pop("http_conn_id", None)
        if http_conn_id:
            warnings.warn(
                "Parameter `http_conn_id` is deprecated. Please use `slack_webhook_conn_id` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if slack_webhook_conn_id:
                raise AirflowException("You cannot provide both `slack_webhook_conn_id` and `http_conn_id`.")
            slack_webhook_conn_id = http_conn_id

        # Compatibility with previous version of operator which based on SimpleHttpOperator.
        # Users might pass these arguments previously, however its never pass to SlackWebhookHook.
        # We remove this arguments if found in ``kwargs`` and notify users if found any.
        deprecated_class_attrs = []
        for deprecated_attr in (
            "endpoint",
            "method",
            "data",
            "headers",
            "response_check",
            "response_filter",
            "extra_options",
            "log_response",
            "auth_type",
            "tcp_keep_alive",
            "tcp_keep_alive_idle",
            "tcp_keep_alive_count",
            "tcp_keep_alive_interval",
        ):
            if deprecated_attr in kwargs:
                deprecated_class_attrs.append(deprecated_attr)
                kwargs.pop(deprecated_attr)
        if deprecated_class_attrs:
            warnings.warn(
                f"Provide {','.join(repr(a) for a in deprecated_class_attrs)} is deprecated "
                f"and as has no affect, please remove it from {self.__class__.__name__} "
                "constructor attributes otherwise in future version of provider it might cause an issue.",
                DeprecationWarning,
                stacklevel=2,
            )

        super().__init__(**kwargs)
        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.webhook_token = webhook_token
        self.proxy = proxy
        self.message = message
        self.attachments = attachments
        self.blocks = blocks
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.icon_url = icon_url
        self.link_names = link_names

    @cached_property
    def hook(self) -> SlackWebhookHook:
        """Create and return an SlackWebhookHook (cached)."""
        return SlackWebhookHook(
            slack_webhook_conn_id=self.slack_webhook_conn_id,
            proxy=self.proxy,
            # Deprecated. SlackWebhookHook will notify user if user provide non-empty ``webhook_token``.
            webhook_token=self.webhook_token,
        )

    def execute(self, context: Context) -> None:
        """Call the SlackWebhookHook to post the provided Slack message"""
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
            # Unused Parameters, if not None than warn user
            link_names=self.link_names,
        )
