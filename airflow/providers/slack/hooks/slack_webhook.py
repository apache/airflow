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

    .. warning::
        This hook intend to use `Slack Webhook` connection
        and might not work correctly with `Slack API` connection.

    Each Slack webhook token can be pre-configured to use a specific channel, username and
    icon. You can override these defaults in this hook.

    :param http_conn_id: connection that has Slack webhook token in the password field
    :param webhook_token: Slack webhook token
    :param message: The message you want to send on Slack
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
    """

    conn_name_attr = 'http_conn_id'
    default_conn_name = 'slack_default'
    conn_type = 'slackwebhook'
    hook_name = 'Slack Webhook'

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
        :param http_conn_id: The conn_id provided
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
