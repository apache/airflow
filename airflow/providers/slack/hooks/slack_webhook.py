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
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable
from urllib.parse import urlparse

from slack_sdk import WebhookClient

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.slack.utils import ConnectionExtraConfig
from airflow.utils.log.secrets_masker import mask_secret

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler

DEFAULT_SLACK_WEBHOOK_ENDPOINT = "https://hooks.slack.com/services"
LEGACY_INTEGRATION_PARAMS = ("channel", "username", "icon_emoji", "icon_url")


def check_webhook_response(func: Callable) -> Callable:
    """Function decorator that check WebhookResponse and raise an error if status code != 200."""

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


def _ensure_prefixes(conn_type):
    """
    Remove when provider min airflow version >= 2.5.0 since this is handled by
    provider manager from that version.
    """

    def dec(func):
        @wraps(func)
        def inner(cls):
            field_behaviors = func(cls)
            conn_attrs = {"host", "schema", "login", "password", "port", "extra"}

            def _ensure_prefix(field):
                if field not in conn_attrs and not field.startswith("extra__"):
                    return f"extra__{conn_type}__{field}"
                else:
                    return field

            if "placeholders" in field_behaviors:
                placeholders = field_behaviors["placeholders"]
                field_behaviors["placeholders"] = {_ensure_prefix(k): v for k, v in placeholders.items()}
            return field_behaviors

        return inner

    return dec


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
    :param webhook_token: (deprecated) Slack Incoming Webhook token.
        Use instead Slack Incoming Webhook connection password field.
    """

    conn_name_attr = "slack_webhook_conn_id"
    default_conn_name = "slack_default"
    conn_type = "slackwebhook"
    hook_name = "Slack Incoming Webhook"

    def __init__(
        self,
        slack_webhook_conn_id: str | None = None,
        webhook_token: str | None = None,
        timeout: int | None = None,
        proxy: str | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **kwargs,
    ):
        super().__init__()

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

        if not slack_webhook_conn_id and not webhook_token:
            raise AirflowException("Either `slack_webhook_conn_id` or `webhook_token` should be provided.")
        if webhook_token:
            mask_secret(webhook_token)
            warnings.warn(
                "Provide `webhook_token` as hook argument deprecated by security reason and will be removed "
                "in a future releases. Please specify it in `Slack Webhook` connection.",
                DeprecationWarning,
                stacklevel=2,
            )
        if not slack_webhook_conn_id:
            warnings.warn(
                "You have not set parameter `slack_webhook_conn_id`. Currently `Slack Incoming Webhook` "
                "connection id optional but in a future release it will mandatory.",
                FutureWarning,
                stacklevel=2,
            )

        self.slack_webhook_conn_id = slack_webhook_conn_id
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers
        self._webhook_token = webhook_token

        # Compatibility with previous version of SlackWebhookHook
        deprecated_class_attrs = []
        for deprecated_attr in (
            "message",
            "attachments",
            "blocks",
            "channel",
            "username",
            "icon_emoji",
            "icon_url",
            "link_names",
        ):
            if deprecated_attr in kwargs:
                deprecated_class_attrs.append(deprecated_attr)
                setattr(self, deprecated_attr, kwargs.pop(deprecated_attr))
                if deprecated_attr == "message":
                    # Slack WebHook Post Request not expected `message` as field,
                    # so we also set "text" attribute which will check by SlackWebhookHook._resolve_argument
                    self.text = getattr(self, deprecated_attr)
                elif deprecated_attr == "link_names":
                    warnings.warn(
                        "`link_names` has no affect, if you want to mention user see: "
                        "https://api.slack.com/reference/surfaces/formatting#mentioning-users",
                        UserWarning,
                        stacklevel=2,
                    )

        if deprecated_class_attrs:
            warnings.warn(
                f"Provide {','.join(repr(a) for a in deprecated_class_attrs)} as hook argument(s) "
                f"is deprecated and will be removed in a future releases. "
                f"Please specify attributes in `{self.__class__.__name__}.send` method instead.",
                DeprecationWarning,
                stacklevel=2,
            )

        self.extra_client_args = kwargs

    @cached_property
    def client(self) -> WebhookClient:
        """Get the underlying slack_sdk.webhook.WebhookClient (cached)."""
        return WebhookClient(**self._get_conn_params())

    def get_conn(self) -> WebhookClient:
        """Get the underlying slack_sdk.webhook.WebhookClient (cached)."""
        return self.client

    @cached_property
    def webhook_token(self) -> str:
        """Return Slack Webhook Token URL."""
        warnings.warn(
            "`SlackHook.webhook_token` property deprecated and will be removed in a future releases.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_conn_params()["url"]

    def _get_conn_params(self) -> dict[str, Any]:
        """Fetch connection params as a dict and merge it with hook parameters."""
        default_schema, _, default_host = DEFAULT_SLACK_WEBHOOK_ENDPOINT.partition("://")
        if self.slack_webhook_conn_id:
            conn = self.get_connection(self.slack_webhook_conn_id)
        else:
            # If slack_webhook_conn_id not specified, then use connection with default schema and host
            conn = Connection(
                conn_id=None, conn_type=self.conn_type, host=default_schema, password=default_host
            )
        extra_config = ConnectionExtraConfig(
            conn_type=self.conn_type,
            conn_id=conn.conn_id,
            extra=conn.extra_dejson,
        )
        conn_params: dict[str, Any] = {"retry_handlers": self.retry_handlers}

        webhook_token = None
        if self._webhook_token:
            self.log.debug("Retrieving Slack Webhook Token from hook attribute.")
            webhook_token = self._webhook_token
        elif conn.conn_id:
            if conn.password:
                self.log.debug(
                    "Retrieving Slack Webhook Token from Connection ID %r password.",
                    self.slack_webhook_conn_id,
                )
                webhook_token = conn.password

        webhook_token = webhook_token or ""
        if not webhook_token and not conn.host:
            raise AirflowException("Cannot get token: No valid Slack token nor valid Connection ID supplied.")
        elif webhook_token and "://" in webhook_token:
            self.log.debug("Retrieving Slack Webhook Token URL from webhook token.")
            url = webhook_token
        else:
            self.log.debug("Constructing Slack Webhook Token URL.")
            if conn.host and "://" in conn.host:
                base_url = conn.host
            else:
                schema = conn.schema if conn.schema else default_schema
                host = conn.host if conn.host else default_host
                base_url = f"{schema}://{host}"

            base_url = base_url.rstrip("/")
            if not webhook_token:
                parsed_token = (urlparse(base_url).path or "").strip("/")
                if base_url == DEFAULT_SLACK_WEBHOOK_ENDPOINT or not parsed_token:
                    # Raise an error in case of password not specified and
                    # 1. Result of constructing base_url equal https://hooks.slack.com/services
                    # 2. Empty url path, e.g. if base_url = https://hooks.slack.com
                    raise AirflowException(
                        "Cannot get token: No valid Slack token nor valid Connection ID supplied."
                    )
                mask_secret(parsed_token)
                warnings.warn(
                    f"Found Slack Webhook Token URL in Connection {conn.conn_id!r} `host` "
                    "and `password` field is empty. This behaviour deprecated "
                    "and could expose you token in the UI and will be removed in a future releases.",
                    DeprecationWarning,
                    stacklevel=2,
                )
            url = (base_url.rstrip("/") + "/" + webhook_token.lstrip("/")).rstrip("/")

        conn_params["url"] = url
        # Merge Hook parameters with Connection config
        conn_params.update(
            {
                "timeout": self.timeout or extra_config.getint("timeout", default=None),
                "proxy": self.proxy or extra_config.get("proxy", default=None),
            }
        )
        # Add additional client args
        conn_params.update(self.extra_client_args)
        if "logger" not in conn_params:
            conn_params["logger"] = self.log

        return {k: v for k, v in conn_params.items() if v is not None}

    def _resolve_argument(self, name: str, value):
        """
        Resolve message parameters.

        .. note::
            This method exist for compatibility and merge instance class attributes with
            method attributes and not be required when assign class attributes to message
            would completely remove.
        """
        if value is None and name in (
            "text",
            "attachments",
            "blocks",
            "channel",
            "username",
            "icon_emoji",
            "icon_url",
            "link_names",
        ):
            return getattr(self, name, None)

        return value

    @check_webhook_response
    def send_dict(self, body: dict[str, Any] | str, *, headers: dict[str, str] | None = None):
        """
        Performs a Slack Incoming Webhook request with given JSON data block.

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
        attachments: list[dict[str, Any]] | None = None,
        blocks: list[dict[str, Any]] | None = None,
        response_type: str | None = None,
        replace_original: bool | None = None,
        delete_original: bool | None = None,
        unfurl_links: bool | None = None,
        unfurl_media: bool | None = None,
        headers: dict[str, str] | None = None,
        **kwargs,
    ):
        """
        Performs a Slack Incoming Webhook request with given arguments.

        :param text: The text message
            (even when having blocks, setting this as well is recommended as it works as fallback).
        :param attachments: A collection of attachments.
        :param blocks: A collection of Block Kit UI components.
        :param response_type: The type of message (either 'in_channel' or 'ephemeral').
        :param replace_original: True if you use this option for response_url requests.
        :param delete_original: True if you use this option for response_url requests.
        :param unfurl_links: Option to indicate whether text url should unfurl.
        :param unfurl_media: Option to indicate whether media url should unfurl.
        :param headers: Request headers for this request.
        """
        body = {
            "text": self._resolve_argument("text", text),
            "attachments": self._resolve_argument("attachments", attachments),
            "blocks": self._resolve_argument("blocks", blocks),
            "response_type": response_type,
            "replace_original": replace_original,
            "delete_original": delete_original,
            "unfurl_links": unfurl_links,
            "unfurl_media": unfurl_media,
            # Legacy Integration Parameters
            **{lip: self._resolve_argument(lip, kwargs.pop(lip, None)) for lip in LEGACY_INTEGRATION_PARAMS},
        }
        if kwargs:
            warnings.warn(
                f"Found unexpected keyword-argument(s) {', '.join(repr(k) for k in kwargs)} "
                "in `send` method. This argument(s) have no effect.",
                UserWarning,
                stacklevel=2,
            )
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
        Performs a Slack Incoming Webhook request with given text.

        :param text: The text message.
        :param unfurl_links: Option to indicate whether text url should unfurl.
        :param unfurl_media: Option to indicate whether media url should unfurl.
        :param headers: Request headers for this request.
        """
        return self.send(text=text, unfurl_links=unfurl_links, unfurl_media=unfurl_media, headers=headers)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns dictionary of widgets to be added for the hook to handle extra values."""
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
    @_ensure_prefixes(conn_type="slackwebhook")
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
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

    def execute(self) -> None:
        """
        Remote Popen (actually execute the slack webhook call).

        .. note::
            This method exist for compatibility with previous version of operator
            and expected that Slack Incoming Webhook message constructing from class attributes rather than
            pass as method arguments.
        """
        warnings.warn(
            "`SlackWebhookHook.execute` method deprecated and will be removed in a future releases. "
            "Please use `SlackWebhookHook.send` or `SlackWebhookHook.send_dict` or "
            "`SlackWebhookHook.send_text` methods instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.send()
