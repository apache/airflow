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
"""
Hook for Slack.

.. spelling:word-list::

    AsyncSlackResponse
"""

from __future__ import annotations

import json
import time
import warnings
from collections.abc import Sequence
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_client import AsyncWebClient
from typing_extensions import NotRequired

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException, BaseHook
from airflow.providers.slack.utils import ConnectionExtraConfig
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler
    from slack_sdk.web.async_client import AsyncSlackResponse
    from slack_sdk.web.slack_response import SlackResponse

    from airflow.providers.common.compat.sdk import Connection


class FileUploadTypeDef(TypedDict):
    """
    Represents the structure of the file upload data.

    :ivar file: Optional. Path to file which need to be sent.
    :ivar content: Optional. File contents. If omitting this parameter, you must provide a file.
    :ivar filename: Optional. Displayed filename.
    :ivar title: Optional. The title of the uploaded file.
    :ivar alt_txt: Optional. Description of image for screen-reader.
    :ivar snippet_type: Optional. Syntax type of the snippet being uploaded.
    """

    file: NotRequired[str | None]
    content: NotRequired[str | None]
    filename: NotRequired[str | None]
    title: NotRequired[str | None]
    alt_txt: NotRequired[str | None]
    snippet_type: NotRequired[str | None]


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

    Additional arguments which are not listed into parameters exposed
    into the rest of ``slack.WebClient`` constructor args.

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

        # Use for caching channels result
        self._channels_mapping: dict[str, str] = {}

    @cached_property
    def client(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        conn = self.get_connection(self.slack_conn_id)
        return WebClient(**self._get_conn_params(conn=conn))

    async def get_async_client(self) -> AsyncWebClient:
        """Get the underlying `slack_sdk.web.async_client.AsyncWebClient`."""
        conn = await get_async_connection(self.slack_conn_id)
        return AsyncWebClient(**self._get_conn_params(conn))

    def get_conn(self) -> WebClient:
        """Get the underlying slack_sdk.WebClient (cached)."""
        return self.client

    def _get_conn_params(self, conn: Connection) -> dict[str, Any]:
        """Fetch connection params as a dict and merge it with hook parameters."""
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
        Call Slack WebClient `WebClient.api_call` with given arguments.

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

    async def async_call(self, api_method: str, **kwargs) -> AsyncSlackResponse:
        """
        Call Slack WebClient `AsyncWebClient.api_call` with given arguments.

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
        client = await self.get_async_client()
        return await client.api_call(api_method, **kwargs)

    def send_file_v2(
        self,
        *,
        channel_id: str | None = None,
        file_uploads: FileUploadTypeDef | list[FileUploadTypeDef],
        initial_comment: str | None = None,
    ) -> SlackResponse:
        """
        Send one or more files to a Slack channel using the Slack SDK Client method `files_upload_v2`.

        :param channel_id: The ID of the channel to send the file to.
            If omitting this parameter, then file will send to workspace.
        :param file_uploads: The file(s) specification to upload.
        :param initial_comment: The message text introducing the file in specified ``channel``.
        """
        if channel_id and channel_id.startswith("#"):
            retried_channel_id = self.get_channel_id(channel_id[1:])
            warnings.warn(
                "The method `files_upload_v2` in the Slack SDK Client expects a Slack Channel ID, "
                f"but received a Slack Channel Name. To resolve this, consider replacing {channel_id!r} "
                f"with the corresponding Channel ID {retried_channel_id!r}.",
                UserWarning,
                stacklevel=2,
            )
            channel_id = retried_channel_id

        if not isinstance(file_uploads, list):
            file_uploads = [file_uploads]
        for file_upload in file_uploads:
            if not file_upload.get("filename"):
                # Some of early version of Slack SDK (such as 3.19.0) raise an error if ``filename`` not set.
                file_upload["filename"] = "Uploaded file"

        return self.client.files_upload_v2(
            channel=channel_id,
            # mypy doesn't happy even if TypedDict used instead of dict[str, Any]
            # see: https://github.com/python/mypy/issues/4976
            file_uploads=file_uploads,  # type: ignore[arg-type]
            initial_comment=initial_comment,
        )

    def send_file_v1_to_v2(
        self,
        *,
        channels: str | Sequence[str] | None = None,
        file: str | Path | None = None,
        content: str | None = None,
        filename: str | None = None,
        initial_comment: str | None = None,
        title: str | None = None,
        snippet_type: str | None = None,
    ) -> list[SlackResponse]:
        """
        Smooth transition between ``send_file`` and ``send_file_v2`` methods.

        :param channels: Comma-separated list of channel names or IDs where the file will be shared.
            If omitting this parameter, then file will send to workspace.
            File would be uploaded for each channel individually.
        :param file: Path to file which need to be sent.
        :param content: File contents. If omitting this parameter, you must provide a file.
        :param filename: Displayed filename.
        :param initial_comment: The message text introducing the file in specified ``channels``.
        :param title: Title of the file.
        :param snippet_type: Syntax type for the content being uploaded.
        """
        if not exactly_one(file, content):
            raise ValueError("Either `file` or `content` must be provided, not both.")
        if file:
            file = Path(file)
            file_uploads: FileUploadTypeDef = {"file": file.__fspath__(), "filename": filename or file.name}
        else:
            file_uploads = {"content": content, "filename": filename}

        file_uploads.update({"title": title, "snippet_type": snippet_type})

        if channels:
            if isinstance(channels, str):
                channels = channels.split(",")
            channels_to_share: list[str | None] = list(map(str.strip, channels))
        else:
            channels_to_share = [None]

        responses = []
        for channel in channels_to_share:
            responses.append(
                self.send_file_v2(
                    channel_id=channel, file_uploads=file_uploads, initial_comment=initial_comment
                )
            )
        return responses

    def get_channel_id(self, channel_name: str) -> str:
        """
        Retrieve a Slack channel id by a channel name.

        It continuously iterates over all Slack channels (public and private)
        until it finds the desired channel name in addition cache results for further usage.

        .. seealso::
            https://api.slack.com/methods/conversations.list

        :param channel_name: The name of the Slack channel for which ID has to be found.
        """
        next_cursor = None
        while not (channel_id := self._channels_mapping.get(channel_name)):
            res = self._call_conversations_list(cursor=next_cursor)
            if TYPE_CHECKING:
                # Slack SDK response type too broad, this should make mypy happy
                assert isinstance(res.data, dict)

            for channel_data in res.data.get("channels", []):
                self._channels_mapping[channel_data["name"]] = channel_data["id"]

            if not (next_cursor := res.data.get("response_metadata", {}).get("next_cursor")):
                channel_id = self._channels_mapping.get(channel_name)
                break

        if not channel_id:
            msg = f"Unable to find slack channel with name: {channel_name!r}"
            raise LookupError(msg)
        return channel_id

    def _call_conversations_list(self, cursor: str | None = None):
        """
        Call ``conversations.list`` with automatic 429-retry.

        .. versionchanged:: 3.0.0
            Automatically retries on 429 responses (up to 5 times, honouring *Retry-After* header).

        :param cursor: Pagination cursor returned by the previous ``conversations.list`` call.
            Pass ``None`` (default) to start from the first page.
        :raises AirflowException: If the method hits the rate-limit 5 times in a row.
        :raises SlackApiError: Propagated when errors other than 429 occur.
        :return: Slack SDK response for the page requested.
        """
        max_retries = 5
        for attempt in range(max_retries):
            try:
                return self.client.conversations_list(cursor=cursor, types="public_channel,private_channel")
            except SlackApiError as e:
                if e.response.status_code == 429 and attempt < max_retries:
                    retry_after = int(e.response.headers.get("Retry-After", 30))
                    self.log.warning(
                        "Rate limit hit. Retrying in %s seconds. Attempt %s/%s",
                        retry_after,
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(retry_after)
                else:
                    raise
        raise AirflowException("Max retries reached for conversations.list")

    def test_connection(self):
        """
        Tests the Slack API connection.

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
        """Return dictionary of widgets to be added for the hook to handle extra values."""
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
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["login", "port", "host", "schema", "extra"],
            "relabeling": {
                "password": "Slack API Token",
            },
            "placeholders": {
                "password": "REPLACE ME WITH A SLACK ACCESS TOKEN",
                "timeout": "30",
                "base_url": "https://www.slack.com/api/",
                "proxy": "http://localhost:9000",
            },
        }
