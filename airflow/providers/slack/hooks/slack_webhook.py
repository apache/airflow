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
#
import json
import warnings
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class SlackWebhookHook(HttpHook):
    """
    This hook allows you to post messages to Slack using incoming webhooks.
    Takes both Slack webhook token directly and connection that has Slack webhook token.
    If both supplied, http_conn_id will be used as base_url,
    and webhook_token will be taken as endpoint, the relative path of the url.

    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.

    :param http_conn_id: connection that has Slack webhook token in the extra field
    :type http_conn_id: str
    :param webhook_token: Slack webhook token
    :type webhook_token: str
    :param message: The message you want to send on Slack
    :type message: str
    :param attachments: The attachments to send on Slack. Should be a list of
        dictionaries representing Slack attachments.
    :type attachments: list
    :param blocks: The blocks to send on Slack. Should be a list of
        dictionaries representing Slack blocks.
    :type blocks: list
    :param channel: The channel the message should be posted to
    :type channel: str
    :param username: The username to post to slack with
    :type username: str
    :param icon_emoji: The emoji to use as icon for the user posting to Slack
    :type icon_emoji: str
    :param icon_url: The icon image URL string to use in place of the default icon.
    :type icon_url: str
    :param link_names: Whether or not to find and link channel and usernames in your
        message
    :type link_names: bool
    :param proxy: Proxy to use to make the Slack webhook call
    :type proxy: str
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        http_conn_id=None,
        webhook_token=None,
        message="",
        attachments=None,
        blocks=None,
        channel=None,
        username=None,
        icon_emoji=None,
        icon_url=None,
        link_names=False,
        proxy=None,
        *args,
        **kwargs,
    ):
        super().__init__(http_conn_id=http_conn_id, *args, **kwargs)
        self.webhook_token = self._get_token(webhook_token, http_conn_id)
        self.message = message
        self.attachments = attachments
        self.blocks = blocks
        self.channel = channel
        self.username = username
        self.icon_emoji = icon_emoji
        self.icon_url = icon_url
        self.link_names = link_names
        self.proxy = proxy

    def _get_token(self, token: str, http_conn_id: Optional[str]) -> str:
        """
        Given either a manually set token or a conn_id, return the webhook_token to use.

        :param token: The manually provided token
        :type token: str
        :param http_conn_id: The conn_id provided
        :type http_conn_id: str
        :return: webhook_token to use
        :rtype: str
        """
        if token:
            return token
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)

            if getattr(conn, 'password', None):
                return conn.password
            else:
                extra = conn.extra_dejson
                web_token = extra.get('webhook_token', '')

                if web_token:
                    warnings.warn(
                        "'webhook_token' in 'extra' is deprecated. Please use 'password' field",
                        DeprecationWarning,
                        stacklevel=2,
                    )

                return web_token
        else:
            raise AirflowException('Cannot get token: No valid Slack webhook token nor conn_id supplied')

    def _build_slack_message(self) -> str:
        """
        Construct the Slack message. All relevant parameters are combined here to a valid
        Slack json message.

        :return: Slack message to send
        :rtype: str
        """
        cmd = {}

        if self.channel:
            cmd['channel'] = self.channel
        if self.username:
            cmd['username'] = self.username
        if self.icon_emoji:
            cmd['icon_emoji'] = self.icon_emoji
        if self.icon_url:
            cmd['icon_url'] = self.icon_url
        if self.link_names:
            cmd['link_names'] = 1
        if self.attachments:
            cmd['attachments'] = self.attachments
        if self.blocks:
            cmd['blocks'] = self.blocks

        cmd['text'] = self.message
        return json.dumps(cmd)

    def execute(self) -> None:
        """Remote Popen (actually execute the slack webhook call)"""
        proxies = {}
        if self.proxy:
            # we only need https proxy for Slack, as the endpoint is https
            proxies = {'https': self.proxy}

        slack_message = self._build_slack_message()
        self.run(
            endpoint=self.webhook_token,
            data=slack_message,
            headers={'Content-type': 'application/json'},
            extra_options={'proxies': proxies, 'check_response': True},
        )
