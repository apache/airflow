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

import json
import warnings
from functools import cached_property, wraps
from typing import TYPE_CHECKING, Any, Callable

from slack_sdk import WebhookClient

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.providers.slack.utils import ConnectionExtraConfig

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler

LEGACY_INTEGRATION_PARAMS = ("channel", "username", "icon_emoji", "icon_url")


def check_webhook_response(func: Callable) -> Callable:
    """Check WebhookResponse and raise an error if status code != 200."""

    @wraps(func)
    def wrapper(*args, **kwargs) -> Callable:
        resp = func(*args, **kwargs)
        if resp.status_code != 200:
            raise AirflowException(
                f"Response body: {resp.body!r}, Status Code: {resp.status_code}. "
                "See: https://api.slack.com/messaging/webhooks#handling_errors"
            )
        return resp

    return wrapper


class SlackWebhookHook(BaseHook):
    """
    This class provide a thin wrapper around the ``slack_sdk.WebhookClient``.

    This hook allows you to post messages to Slack by using Incoming Webhooks.

    .. seealso::
        - :ref:`Slack Incoming Webhook connection <howto/connection:slack-incoming-webhook>`
        - https://api.slack.com/messaging/webhooks
        - https://slack.dev/python-slack-sdk/webhook/index.html

    .. note::
        You cannot override the default channel (chosen by the user who installed your app),
        username, or icon when you're using Incoming Webhooks to post messages.
        Instead, these values will always inherit from the associated Slack App configuration
        (`link <https://api.slack.com/messaging/webhooks#advanced_message_formatting>`_).
        It is possible to change this values only in `Legacy Slack Integration Incoming Webhook
        <https://api.slack.com/legacy/custom-integrations/messaging/webhooks#legacy-customizations>`_.

    .. warning::
        This hook intend to use `Slack Incoming Webhook` connection
        and might not work correctly with `Slack API` connection.

    Examples:
     .. code-block:: python

        # Create hook
        hook = SlackWebhookHook(slack_webhook_conn_id="slack_default")

        # Post message in Slack channel by JSON formatted message
        # See: https://api.slack.com/messaging/webhooks#posting_with_webhooks
        hook.send_dict({"text": "Hello world!"})

        # Post simple message in Slack channel
        hook.send_text("Hello world!")

        # Use ``slack_sdk.WebhookClient``
        hook.client.send(text="Hello world!")

    :param slack_webhook_conn_id: Slack Incoming Webhook connection id
        that has Incoming Webhook token in the password field.
    :param timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. If not set than default WebhookClient value will use.
    :param proxy: Proxy to make the Slack Incoming Webhook call.
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebhookClient``.
    """

    conn_name_attr = "slack_webhook_conn_id"
    default_conn_name = "slack_default"
    conn_type = "slackwebhook"
    hook_name = "Slack Incoming Webhook"

    def __init__(
        self,
        *,
        slack_webhook_conn_id: str,
        timeout: int | None = None,
        proxy: str | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **extra_client_args: Any,
    ):
        super().__init__()
        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers
        if "webhook_token" in extra_client_args:
            warnings.warn(
                f"Provide `webhook_token` as part of {type(self).__name__!r} parameters is disallowed, "
                f"please use Airflow Connection.",
                UserWarning,
                stacklevel=2,
            )
            extra_client_args.pop("webhook_token")
        if "logger" not in extra_client_args:
            extra_client_args["logger"] = self.log
        self.extra_client_args = extra_client_args

    @cached_property
    def client(self) -> WebhookClient:
        """Get the underlying slack_sdk.webhook.WebhookClient (cached)."""
        return WebhookClient(**self._get_conn_params())

    def get_conn(self) -> WebhookClient:
        """Get the underlying slack_sdk.webhook.WebhookClient (cached)."""
        return self.client

    def _get_conn_params(self) -> dict[str, Any]:
        """Fetch connection params as a dict and merge it with hook parameters."""
        conn = self.get_connection(self.slack_webhook_conn_id)
        if not conn.password or not conn.password.strip():
            raise AirflowNotFoundException(
                f"Connection ID {self.slack_webhook_conn_id!r} does not contain password "
                f"(Slack Webhook Token)."
            )

        webhook_token = conn.password.strip()
        if webhook_token and "://" in conn.password:
            self.log.debug("Retrieving Slack Webhook Token URL from webhook token.")
            url = webhook_token
        else:
            self.log.debug("Constructing Slack Webhook Token URL.")
            base_url = conn.host
            if not base_url or "://" not in base_url:
                base_url = f"{conn.schema or 'https'}://{conn.host or 'hooks.slack.com/services'}"
            url = (base_url.rstrip("/") + "/" + webhook_token.lstrip("/")).rstrip("/")

        conn_params: dict[str, Any] = {"url": url, "retry_handlers": self.retry_handlers}
        extra_config = ConnectionExtraConfig(
            conn_type=self.conn_type, conn_id=conn.conn_id, extra=conn.extra_dejson
        )
        # Merge Hook parameters with Connection config
        conn_params.update(
            {
                "timeout": self.timeout or extra_config.getint("timeout", default=None),
                "proxy": self.proxy or extra_config.get("proxy", default=None),
            }
        )
        # Add additional client args
        conn_params.update(self.extra_client_args)
        return {k: v for k, v in conn_params.items() if v is not None}

    @check_webhook_response
    def send_dict(self, body: dict[str, Any] | str, *, headers: dict[str, str] | None = None):
        """
        Perform a Slack Incoming Webhook request with given JSON data block.

        :param body: JSON data structure, expected dict or JSON-string.
        :param headers: Request headers for this request.
        """
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except json.JSONDecodeError as err:
                raise AirflowException(
                    f"Body expected valid JSON string, got {body!r}. Original error:\n * {err}"
                ) from None

        if not isinstance(body, dict):
            raise TypeError(f"Body expected dictionary, got {type(body).__name__}.")

        if any(legacy_attr in body for legacy_attr in ("channel", "username", "icon_emoji", "icon_url")):
            warnings.warn(
                "You cannot override the default channel (chosen by the user who installed your app), "
                "username, or icon when you're using Incoming Webhooks to post messages. "
                "Instead, these values will always inherit from the associated Slack app configuration. "
                "See: https://api.slack.com/messaging/webhooks#advanced_message_formatting. "
                "It is possible to change this values only in Legacy Slack Integration Incoming Webhook: "
                "https://api.slack.com/legacy/custom-integrations/messaging/webhooks#legacy-customizations",
                UserWarning,
                stacklevel=2,
            )

        return self.client.send_dict(body, headers=headers)

    def send(
        self,
        *,
        text: str | None = None,
        blocks: list[dict[str, Any]] | None = None,
        response_type: str | None = None,
        replace_original: bool | None = None,
        delete_original: bool | None = None,
        unfurl_links: bool | None = None,
        unfurl_media: bool | None = None,
        headers: dict[str, str] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        **kwargs,
    ):
        """
        Perform a Slack Incoming Webhook request with given arguments.

        :param text: The text message
            (even when having blocks, setting this as well is recommended as it works as fallback).
        :param blocks: A collection of Block Kit UI components.
        :param response_type: The type of message (either 'in_channel' or 'ephemeral').
        :param replace_original: True if you use this option for response_url requests.
        :param delete_original: True if you use this option for response_url requests.
        :param unfurl_links: Option to indicate whether text url should unfurl.
        :param unfurl_media: Option to indicate whether media url should unfurl.
        :param headers: Request headers for this request.
        :param attachments: (legacy) A collection of attachments.
        """
        body = {
            "text": text,
            "attachments": attachments,
            "blocks": blocks,
            "response_type": response_type,
            "replace_original": replace_original,
            "delete_original": delete_original,
            "unfurl_links": unfurl_links,
            "unfurl_media": unfurl_media,
            # Legacy Integration Parameters
            **kwargs,
        }
        body = {k: v for k, v in body.items() if v is not None}
        return self.send_dict(body=body, headers=headers)

    def send_text(
        self,
        text: str,
        *,
        unfurl_links: bool | None = None,
        unfurl_media: bool | None = None,
        headers: dict[str, str] | None = None,
    ):
        """
        Perform a Slack Incoming Webhook request with given text.

        :param text: The text message.
        :param unfurl_links: Option to indicate whether text url should unfurl.
        :param unfurl_media: Option to indicate whether media url should unfurl.
        :param headers: Request headers for this request.
        """
        return self.send(text=text, unfurl_links=unfurl_links, unfurl_media=unfurl_media, headers=headers)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return dictionary of widgets to be added for the hook to handle extra values."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import IntegerField, StringField
        from wtforms.validators import NumberRange, Optional

        return {
            "timeout": IntegerField(
                lazy_gettext("Timeout"),
                widget=BS3TextFieldWidget(),
                validators=[Optional(), NumberRange(min=1)],
                description="Optional. The maximum number of seconds the client will wait to connect "
                "and receive a response from Slack Incoming Webhook.",
            ),
            "proxy": StringField(
                lazy_gettext("Proxy"),
                widget=BS3TextFieldWidget(),
                description="Optional. Proxy to make the Slack Incoming Webhook call.",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["login", "port", "extra"],
            "relabeling": {
                "host": "Slack Webhook Endpoint",
                "password": "Webhook Token",
            },
            "placeholders": {
                "schema": "https",
                "host": "hooks.slack.com/services",
                "password": "T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX",
                "timeout": "30",
                "proxy": "http://localhost:9000",
            },
        }
