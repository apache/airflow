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
import json
from typing import Any, Dict, List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.slack.hooks.slack import SlackHook


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
        slack_conn_id: Optional[str] = None,
        token: Optional[str] = None,
        method: Optional[str] = None,
        api_params: Optional[Dict] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.token = token  # type: Optional[str]
        self.slack_conn_id = slack_conn_id  # type: Optional[str]

        self.method = method
        self.api_params = api_params

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
        """
        The SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        if not self.api_params:
            self.construct_api_call_params()
        slack = SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)
        slack.call(self.method, json=self.api_params)


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

    template_fields: Sequence[str] = ('username', 'text', 'attachments', 'blocks', 'channel')
    ui_color = '#FFBA40'

    def __init__(
        self,
        channel: str = '#general',
        username: str = 'Airflow',
        text: str = 'No message has been set.\n'
        'Here is a cat video instead\n'
        'https://www.youtube.com/watch?v=J---aiyznGQ',
        icon_url: str = 'https://raw.githubusercontent.com/apache/'
        'airflow/main/airflow/www/static/pin_100.png',
        attachments: Optional[List] = None,
        blocks: Optional[List] = None,
        **kwargs,
    ) -> None:
        self.method = 'chat.postMessage'
        self.channel = channel
        self.username = username
        self.text = text
        self.icon_url = icon_url
        self.attachments = attachments or []
        self.blocks = blocks or []
        super().__init__(method=self.method, **kwargs)

    def construct_api_call_params(self) -> Any:
        self.api_params = {
            'channel': self.channel,
            'username': self.username,
            'text': self.text,
            'icon_url': self.icon_url,
            'attachments': json.dumps(self.attachments),
            'blocks': json.dumps(self.blocks),
        }


class SlackAPIFileOperator(SlackAPIOperator):
    """
    Send a file to a slack channel
    Examples:

    .. code-block:: python

        # Send file with filename and filetype
        slack_operator_file = SlackAPIFileOperator(
            task_id="slack_file_upload_1",
            dag=dag,
            slack_conn_id="slack",
            channel="#general",
            initial_comment="Hello World!",
            filename="/files/dags/test.txt",
            filetype="txt",
        )

        # Send file content
        slack_operator_file_content = SlackAPIFileOperator(
            task_id="slack_file_upload_2",
            dag=dag,
            slack_conn_id="slack",
            channel="#general",
            initial_comment="Hello World!",
            content="file content in txt",
        )

    :param channel: channel in which to sent file on slack name (templated)
    :param initial_comment: message to send to slack. (templated)
    :param filename: name of the file (templated)
    :param filetype: slack filetype. (templated)
        - see https://api.slack.com/types/file
    :param content: file content. (templated)
    """

    template_fields: Sequence[str] = ('channel', 'initial_comment', 'filename', 'filetype', 'content')
    ui_color = '#44BEDF'

    def __init__(
        self,
        channel: str = '#general',
        initial_comment: str = 'No message has been set!',
        filename: Optional[str] = None,
        filetype: Optional[str] = None,
        content: Optional[str] = None,
        **kwargs,
    ) -> None:
        self.method = 'files.upload'
        self.channel = channel
        self.initial_comment = initial_comment
        self.filename = filename
        self.filetype = filetype
        self.content = content
        self.file_params: Dict = {}
        super().__init__(method=self.method, **kwargs)

    def execute(self, **kwargs):
        """
        The SlackAPIOperator calls will not fail even if the call is not unsuccessful.
        It should not prevent a DAG from completing in success
        """
        slack = SlackHook(token=self.token, slack_conn_id=self.slack_conn_id)

        # If file content is passed.
        if self.content is not None:
            self.api_params = {
                'channels': self.channel,
                'content': self.content,
                'initial_comment': self.initial_comment,
            }
            slack.call(self.method, data=self.api_params)
        # If file name is passed.
        elif self.filename is not None:
            self.api_params = {
                'channels': self.channel,
                'filename': self.filename,
                'filetype': self.filetype,
                'initial_comment': self.initial_comment,
            }
            with open(self.filename, "rb") as file_handle:
                slack.call(self.method, data=self.api_params, files={'file': file_handle})
                file_handle.close()
