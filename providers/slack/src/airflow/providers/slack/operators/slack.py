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
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, Literal

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook

if TYPE_CHECKING:
    from slack_sdk.http_retry import RetryHandler

    from airflow.providers.common.compat.sdk import Context


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator class.

    :param slack_conn_id: :ref:`Slack API Connection <howto/connection:slack>`
        which its password is Slack API token.
    :param method: The Slack API Method to Call (https://api.slack.com/methods).
    :param api_params: API Method call parameters (https://api.slack.com/methods). Optional
    :param timeout: The maximum number of seconds the client will wait to connect
        and receive a response from Slack. Optional
    :param base_url: A string representing the Slack API base URL. Optional
    :param proxy: Proxy to make the Slack API call. Optional
    :param retry_handlers: List of handlers to customize retry logic in ``slack_sdk.WebClient``. Optional
    """

    def __init__(
        self,
        *,
        slack_conn_id: str = SlackHook.default_conn_name,
        method: str,
        api_params: dict | None = None,
        base_url: str | None = None,
        proxy: str | None = None,
        timeout: int | None = None,
        retry_handlers: list[RetryHandler] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.slack_conn_id = slack_conn_id
        self.method = method
        self.api_params = api_params
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

    def construct_api_call_params(self) -> Any:
        """
        Construct API call parameters used by the execute function.

        Allow templating on the source fields of the ``api_call_params`` dict
        before construction.

        Child classes should override this. Each SlackAPIOperator child class is
        responsible for having function set ``self.api_call_params`` with a dict
        of API call parameters (https://api.slack.com/methods)
        """
        raise NotImplementedError(
            "SlackAPIOperator should not be used directly. Chose one of the subclasses instead"
        )

    def execute(self, context: Context):
        if not self.method:
            msg = f"Expected non empty `method` attribute in {type(self).__name__!r}, but got {self.method!r}"
            raise ValueError(msg)
        if not self.api_params:
            self.construct_api_call_params()
        self.hook.call(self.method, json=self.api_params)


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Post messages to a Slack channel.

    .. code-block:: python

        slack = SlackAPIPostOperator(
            task_id="post_hello",
            dag=dag,
            text="hello there!",
            channel="#random",
        )

    :param channel: channel in which to post message on slack name (#general) or
        ID (C12318391). (templated)
    :param username: Username that airflow will be posting to Slack as. (templated)
    :param text: message to send to slack. (templated)
    :param icon_url: URL to icon used for this message
    :param blocks: A list of blocks to send with the message. (templated)
        See https://api.slack.com/reference/block-kit/blocks
    :param attachments: (legacy) A list of attachments to send with the message. (templated)
        See https://api.slack.com/docs/attachments
    """

    template_fields: Sequence[str] = ("username", "text", "attachments", "blocks", "channel")
    ui_color = "#FFBA40"

    def __init__(
        self,
        channel: str = "#general",
        username: str = "Airflow",
        text: str = (
            "No message has been set.\n"
            "Here is a cat video instead\n"
            "https://www.youtube.com/watch?v=J---aiyznGQ"
        ),
        icon_url: str = (
            "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/src/airflow/ui/public/pin_100.png"
        ),
        blocks: list | None = None,
        attachments: list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(method="chat.postMessage", **kwargs)
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []

    def construct_api_call_params(self) -> Any:
        self.api_params = {
            "channel": self.channel,
            "username": self.username,
            "text": self.text,
            "icon_url": self.icon_url,
            "attachments": json.dumps(self.attachments),
            "blocks": json.dumps(self.blocks),
        }


class SlackAPIFileOperator(SlackAPIOperator):
    """
    Send a file to a Slack channel.

    .. code-block:: python

        # Send file with filename and filetype
        slack_operator_file = SlackAPIFileOperator(
            task_id="slack_file_upload_1",
            dag=dag,
            slack_conn_id="slack",
            channels="#general,#random",
            initial_comment="Hello World!",
            filename="/files/dags/test.txt",
            filetype="txt",
        )

        # Send file content
        slack_operator_file_content = SlackAPIFileOperator(
            task_id="slack_file_upload_2",
            dag=dag,
            slack_conn_id="slack",
            channels="#general",
            initial_comment="Hello World!",
            content="file content in txt",
        )

    :param channels: Comma-separated list of channel names or IDs where the file will be shared.
        If set this argument to None, then file will send to associated workspace. (templated)
    :param initial_comment: message to send to slack. (templated)
    :param filename: name of the file (templated)
    :param filetype: slack filetype. (templated) See: https://api.slack.com/types/file#file_types
    :param content: file content. (templated)
    :param title: title of file. (templated)
    :param snippet_type: Syntax type for the snippet being uploaded.(templated)
    :param method_version: The version of the method of Slack SDK Client to be used, either "v1" or "v2".
    """

    template_fields: Sequence[str] = (
        "channels",
        "initial_comment",
        "filename",
        "filetype",
        "content",
        "title",
        "snippet_type",
    )
    ui_color = "#44BEDF"

    def __init__(
        self,
        channels: str | Sequence[str] | None = None,
        initial_comment: str | None = None,
        filename: str | None = None,
        filetype: str | None = None,
        content: str | None = None,
        title: str | None = None,
        method_version: Literal["v1", "v2"] | None = None,
        snippet_type: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(method="files.upload", **kwargs)
        self.channels = channels
        self.initial_comment = initial_comment
        self.filename = filename
        self.filetype = filetype
        self.content = content
        self.title = title
        self.method_version = method_version
        self.snippet_type = snippet_type

        if self.filetype:
            warnings.warn(
                "The property `filetype` is no longer supported in slack_sdk and will be removed in a future release.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

        if self.method_version:
            warnings.warn(
                "The property `method_version` is no longer required for `SlackAPIFileOperator`, as slack_sdk is using the files_upload_v2 method by default.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

    def execute(self, context: Context):
        self.hook.send_file_v1_to_v2(
            channels=self.channels,
            # For historical reason SlackAPIFileOperator use filename as reference to file
            file=self.filename,
            content=self.content,
            initial_comment=self.initial_comment,
            title=self.title,
            snippet_type=self.snippet_type,
        )
