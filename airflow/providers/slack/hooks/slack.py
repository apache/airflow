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
from pathlib import Path
from typing import TYPE_CHECKING, Any, Sequence

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from airflow.compat.functools import cached_property
from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.providers.slack.utils import ConnectionExtraConfig, prefixed_extra_field
from airflow.utils.log.secrets_masker import mask_secret

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
    :param token: (deprecated) Slack API Token.
    """

    conn_name_attr = 'slack_conn_id'
    default_conn_name = 'slack_api_default'
    conn_type = 'slack'
    hook_name = 'Slack API'

    def __init__(
        self,
        token: str | None = None,
        slack_conn_id: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
        proxy: str | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **extra_client_args: Any,
    ) -> None:
        if not token and not slack_conn_id:
            raise AirflowException("Either `slack_conn_id` or `token` should be provided.")
        if token:
            mask_secret(token)
            warnings.warn(
                "Provide token as hook argument deprecated by security reason and will be removed "
                "in a future releases. Please specify token in `Slack API` connection.",
                DeprecationWarning,
                stacklevel=2,
            )
        if not slack_conn_id:
            warnings.warn(
                "You have not set parameter `slack_conn_id`. Currently `Slack API` connection id optional "
                "but in a future release it will mandatory.",
                FutureWarning,
                stacklevel=2,
            )

        super().__init__()
        self._token = token
        self.slack_conn_id = slack_conn_id
        self.base_url = base_url
        self.timeout = timeout
        self.proxy = proxy
        self.retry_handlers = retry_handlers
        self.extra_client_args = extra_client_args
        if self.extra_client_args.pop("use_session", None) is not None:
            warnings.warn("`use_session` has no affect in slack_sdk.WebClient.", UserWarning, stacklevel=2)

    @cached_property
    def client(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        return WebClient(**self._get_conn_params())

    def get_conn(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        return self.client

    def _get_conn_params(self) -> dict[str, Any]:
        """Fetch connection params as a dict and merge it with hook parameters."""
        conn = self.get_connection(self.slack_conn_id) if self.slack_conn_id else None
        conn_params: dict[str, Any] = {"retry_handlers": self.retry_handlers}

        if self._token:
            conn_params["token"] = self._token
        elif conn:
            if not conn.password:
                raise AirflowNotFoundException(
                    f"Connection ID {self.slack_conn_id!r} does not contain password (Slack API Token)."
                )
            conn_params["token"] = conn.password

        extra_config = ConnectionExtraConfig(
            conn_type=self.conn_type,
            conn_id=conn.conn_id if conn else None,
            extra=conn.extra_dejson if conn else {},
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
        if "logger" not in conn_params:
            conn_params["logger"] = self.log

        return {k: v for k, v in conn_params.items() if v is not None}

    @cached_property
    def token(self) -> str:
        warnings.warn(
            "`SlackHook.token` property deprecated and will be removed in a future releases.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._get_conn_params()["token"]

    def __get_token(self, token: Any, slack_conn_id: Any) -> str:
        warnings.warn(
            "`SlackHook.__get_token` method deprecated and will be removed in a future releases.",
            DeprecationWarning,
            stacklevel=2,
        )
        if token is not None:
            return token

        if slack_conn_id is not None:
            conn = self.get_connection(slack_conn_id)

            if not getattr(conn, 'password', None):
                raise AirflowException('Missing token(password) in Slack connection')
            return conn.password

        raise AirflowException('Cannot get token: No valid Slack token nor slack_conn_id supplied.')

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
        if not ((not file) ^ (not content)):
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

        return {
            prefixed_extra_field("timeout", cls.conn_type): IntegerField(
                lazy_gettext("Timeout"),
                widget=BS3TextFieldWidget(),
                description="Optional. The maximum number of seconds the client will wait to connect "
                "and receive a response from Slack API.",
            ),
            prefixed_extra_field("base_url", cls.conn_type): StringField(
                lazy_gettext('Base URL'),
                widget=BS3TextFieldWidget(),
                description="Optional. A string representing the Slack API base URL.",
            ),
            prefixed_extra_field("proxy", cls.conn_type): StringField(
                lazy_gettext('Proxy'),
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
                prefixed_extra_field("timeout", cls.conn_type): "30",
                prefixed_extra_field("base_url", cls.conn_type): "https://www.slack.com/api/",
                prefixed_extra_field("proxy", cls.conn_type): "http://localhost:9000",
            },
        }
