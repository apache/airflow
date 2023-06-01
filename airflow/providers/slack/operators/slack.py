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
from typing import Any, Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.utils.log.secrets_masker import mask_secret


class SlackAPIOperator(BaseOperator):
    """
    Base Slack Operator
    The SlackAPIPostOperator is derived from this operator.
    In the future additional Slack API Operators will be derived from this class as well.
    Only one of `slack_conn_id` and `token` is required.

    :param slack_conn_id: :ref:`Slack API Connection <howto/connection:slack>`
        which its password is Slack API token. Optional
    :param token: Slack API token (https://api.slack.com/web). Optional
    :param method: The Slack API Method to Call (https://api.slack.com/methods). Optional
    :param api_params: API Method call parameters (https://api.slack.com/methods). Optional
    :param client_args: Slack Hook parameters. Optional. Check airflow.providers.slack.hooks.SlackHook
    """

    def __init__(
        self,
        *,
        slack_conn_id: str | None = None,
        token: str | None = None,
        method: str | None = None,
        api_params: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if token:
            mask_secret(token)
        self.token = token
        self.slack_conn_id = slack_conn_id

        self.method = method
        self.api_params = api_params

    @cached_property
    def hook(self) -> SlackHook:
        """Slack Hook."""
        return SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)

    def construct_api_call_params(self) -> Any:
        """
        Used by the execute function. Allows templating on the source fields
        of the api_call_params dict before construction

        Override in child classes.
        Each SlackAPIOperator child class is responsible for
        having a construct_api_call_params function
        which sets self.api_call_params with a dict of
        API call parameters (https://api.slack.com/methods)
        """
        raise NotImplementedError(
            "SlackAPIOperator should not be used directly. Chose one of the subclasses instead"
        )

    def execute(self, **kwargs):
        if not self.api_params:
            self.construct_api_call_params()
        self.hook.call(self.method, json=self.api_params)


class SlackAPIPostOperator(SlackAPIOperator):
    """
    Posts messages to a slack channel
    Examples:

    .. code-block:: python

        slack = SlackAPIPostOperator(
            task_id="post_hello",
            dag=dag,
            token="XXX",
            text="hello there!",
            channel="#random",
        )

    :param channel: channel in which to post message on slack name (#general) or
        ID (C12318391). (templated)
    :param username: Username that airflow will be posting to Slack as. (templated)
    :param text: message to send to slack. (templated)
    :param icon_url: url to icon used for this message
    :param attachments: extra formatting details. (templated)
        - see https://api.slack.com/docs/attachments.
    :param blocks: extra block layouts. (templated)
        - see https://api.slack.com/reference/block-kit/blocks.
    """

    template_fields: Sequence[str] = ("username", "text", "attachments", "blocks", "channel")
    ui_color = "#FFBA40"

    def __init__(
        self,
        channel: str = "#general",
        username: str = "Airflow",
        text: str = "No message has been set.\n"
        "Here is a cat video instead\n"
        "https://www.youtube.com/watch?v=J---aiyznGQ",
        icon_url: str = "https://raw.githubusercontent.com/apache/"
        "airflow/main/airflow/www/static/pin_100.png",
        attachments: list | None = None,
        blocks: list | None = None,
        **kwargs,
    ) -> None:
        self.method = "chat.postMessage"
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []
        super().__init__(method=self.method, **kwargs)

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
    Send a file to a slack channels
    Examples:

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
    :param channel: (deprecated) channel in which to sent file on slack name
    """

    template_fields: Sequence[str] = (
        "channels",
        "initial_comment",
        "filename",
        "filetype",
        "content",
        "title",
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
        channel: str | None = None,
        **kwargs,
    ) -> None:
        if channel:
            warnings.warn(
                "Argument `channel` is deprecated and will removed in a future releases. "
                "Please use `channels` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            if channels:
                raise ValueError(f"Cannot set both arguments: channel={channel!r} and channels={channels!r}.")
            channels = channel

        self.channels = channels
        self.initial_comment = initial_comment
        self.filename = filename
        self.filetype = filetype
        self.content = content
        self.title = title
        super().__init__(method="files.upload", **kwargs)

    def execute(self, **kwargs):
        self.hook.send_file(
            channels=self.channels,
            # For historical reason SlackAPIFileOperator use filename as reference to file
            file=self.filename,
            content=self.content,
            initial_comment=self.initial_comment,
            title=self.title,
        )
