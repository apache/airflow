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
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.providers.slack.utils import ConnectionExtraConfig
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler
    from slack_sdk.web.slack_response import SlackResponse


class SlackHook(BaseHook):
    """
    Creates a Slack API Connection to be used for calls.

    This class provide a thin wrapper around the ``slack_sdk.WebClient``.

    .. seealso::
        - :ref:`Slack API connection <howto/connection:slack>`
        - https://api.slack.com/messaging
        - https://slack.dev/python-slack-sdk/web/index.html

    .. warning::
        This hook intend to use `Slack API` connection
        and might not work correctly with `Slack Incoming Webhook` and `HTTP` connections.

    Takes both Slack API token directly and connection that has Slack API token. If both are
    supplied, Slack API token will be used. Also exposes the rest of slack.WebClient args.

    Examples:
     .. code-block:: python

        # Create hook
        slack_hook = SlackHook(slack_conn_id="slack_api_default")

        # Call generic API with parameters (errors are handled by hook)
        #  For more details check https://api.slack.com/methods/chat.postMessage
        slack_hook.call("chat.postMessage", json={"channel": "#random", "text": "Hello world!"})

        # Call method from Slack SDK (you have to handle errors yourself)
        #  For more details check https://slack.dev/python-slack-sdk/web/index.html#messaging
        slack_hook.client.chat_postMessage(channel="#random", text="Hello world!")

    :param slack_conn_id: :ref:`Slack connection id <howto/connection:slack>`
        that has Slack API token in the password field.
    :param timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. If not set than default WebClient value will use.
    :param base_url: A string representing the Slack API base URL.
        If not set than default WebClient BASE_URL will use (``https://www.slack.com/api/``).
    :param proxy: Proxy to make the Slack API call.
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebClient``.
    """

    conn_name_attr = "slack_conn_id"
    default_conn_name = "slack_api_default"
    conn_type = "slack"
    hook_name = "Slack API"

    def __init__(
        self,
        *,
        slack_conn_id: str = default_conn_name,
        base_url: str | None = None,
        timeout: int | None = None,
        proxy: str | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **extra_client_args: Any,
    ) -> None:
        super().__init__()
        self.slack_conn_id = slack_conn_id
        self.base_url = base_url
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers
        if "token" in extra_client_args:
            warnings.warn(
                f"Provide `token` as part of {type(self).__name__!r} parameters is disallowed, "
                f"please use Airflow Connection.",
                UserWarning,
                stacklevel=2,
            )
            extra_client_args.pop("token")
        if "logger" not in extra_client_args:
            extra_client_args["logger"] = self.log
        self.extra_client_args = extra_client_args

    @cached_property
    def client(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        return WebClient(**self._get_conn_params())

    def get_conn(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        return self.client

    def _get_conn_params(self) -> dict[str, Any]:
        """Fetch connection params as a dict and merge it with hook parameters."""
        conn = self.get_connection(self.slack_conn_id)
        if not conn.password:
            raise AirflowNotFoundException(
                f"Connection ID {self.slack_conn_id!r} does not contain password (Slack API Token)."
            )
        conn_params: dict[str, Any] = {"token": conn.password, "retry_handlers": self.retry_handlers}
        extra_config = ConnectionExtraConfig(
            conn_type=self.conn_type, conn_id=conn.conn_id, extra=conn.extra_dejson
        )
        # Merge Hook parameters with Connection config
        conn_params.update(
            {
                "timeout": self.timeout or extra_config.getint("timeout", default=None),
                "base_url": self.base_url or extra_config.get("base_url", default=None),
                "proxy": self.proxy or extra_config.get("proxy", default=None),
            }
        )
        # Add additional client args
        conn_params.update(self.extra_client_args)
        return {k: v for k, v in conn_params.items() if v is not None}

    def call(self, api_method: str, **kwargs) -> SlackResponse:
        """
        Calls Slack WebClient `WebClient.api_call` with given arguments.

        :param api_method: The target Slack API method. e.g. 'chat.postMessage'. Required.
        :param http_verb: HTTP Verb. Optional (defaults to 'POST')
        :param files: Files to multipart upload. e.g. {imageORfile: file_objectORfile_path}
        :param data: The body to attach to the request. If a dictionary is provided,
            form-encoding will take place. Optional.
        :param params: The URL parameters to append to the URL. Optional.
        :param json: JSON for the body to attach to the request. Optional.
        :return: The server's response to an HTTP request. Data from the response can be
            accessed like a dict.  If the response included 'next_cursor' it can be
            iterated on to execute subsequent requests.
        """
        return self.client.api_call(api_method, **kwargs)

    def send_file(
        self,
        *,
        channels: str | Sequence[str] | None = None,
        file: str | Path | None = None,
        content: str | None = None,
        filename: str | None = None,
        filetype: str | None = None,
        initial_comment: str | None = None,
        title: str | None = None,
    ) -> SlackResponse:
        """
        Create or upload an existing file.

        :param channels: Comma-separated list of channel names or IDs where the file will be shared.
            If omitting this parameter, then file will send to workspace.
        :param file: Path to file which need to be sent.
        :param content: File contents. If omitting this parameter, you must provide a file.
        :param filename: Displayed filename.
        :param filetype: A file type identifier.
        :param initial_comment: The message text introducing the file in specified ``channels``.
        :param title: Title of file.

        .. seealso::
            - `Slack API files.upload method <https://api.slack.com/methods/files.upload>`_
            - `File types <https://api.slack.com/types/file#file_types>`_
        """
        if not exactly_one(file, content):
            raise ValueError("Either `file` or `content` must be provided, not both.")
        elif file:
            file = Path(file)
            with open(file, "rb") as fp:
                if not filename:
                    filename = file.name
                return self.client.files_upload(
                    file=fp,
                    filename=filename,
                    filetype=filetype,
                    initial_comment=initial_comment,
                    title=title,
                    channels=channels,
                )

        return self.client.files_upload(
            content=content,
            filename=filename,
            filetype=filetype,
            initial_comment=initial_comment,
            title=title,
            channels=channels,
        )

    def test_connection(self):
        """Tests the Slack API connection.

        .. seealso::
            https://api.slack.com/methods/auth.test
        """
        try:
            response = self.call("auth.test")
            response.validate()
        except SlackApiError as e:
            return False, str(e)
        except Exception as e:
            return False, f"Unknown error occurred while testing connection: {e}"

        if isinstance(response.data, bytes):
            # If response data binary then return simple message
            return True, f"Connection successfully tested (url: {response.api_url})."

        try:
            return True, json.dumps(response.data)
        except TypeError:
            return True, str(response)

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
                validators=[Optional(strip_whitespace=True), NumberRange(min=1)],
                description="Optional. The maximum number of seconds the client will wait to connect "
                "and receive a response from Slack API.",
            ),
            "base_url": StringField(
                lazy_gettext("Base URL"),
                widget=BS3TextFieldWidget(),
                description="Optional. A string representing the Slack API base URL.",
            ),
            "proxy": StringField(
                lazy_gettext("Proxy"),
                widget=BS3TextFieldWidget(),
                description="Optional. Proxy to make the Slack API call.",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["login", "port", "host", "schema", "extra"],
            "relabeling": {
                "password": "Slack API Token",
            },
            "placeholders": {
                "password": "xoxb-1234567890123-09876543210987-AbCdEfGhIjKlMnOpQrStUvWx",
                "timeout": "30",
                "base_url": "https://www.slack.com/api/",
                "proxy": "http://localhost:9000",
            },
        }
