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
from typing import Optional

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


class TeamsWebhookHook(HttpHook):
    """
    This hook allows you to post messages to MS Teams using the incoming webhooks.
    Takes both MS Teams webhook token directly and connection that has MS Teams webhook token.
    If both supplied, the webhook token will be appended to the host in the connection.

    :param http_conn_id: connection that has MS Teams webhook URL
    :param webhook_token: MS Teams webhook token
    :param message: The message you want to send on MS Teams
    :param facts: The facts to send on MS Teams. Should be a list of
        dictionaries of two keys representing more details to the message.
        E.g {"name": "Status", "value": "Not started"}
    :param subtitle: The subtitle of the message to send
    :param action_button_name: The name of the action button
    :param action_button_url: The URL for the action button clicked
    :param theme_color: Hex code of the card theme, without the #
    :param icon_url: The icon URL string to be added to message card.
    :param proxy: Proxy to use to make the MS Teams webhook call
    """

    def __init__(
        self,
        http_conn_id: str = "",
        webhook_token: Optional[str] = "",
        message: Optional[str] = "",
        subtitle: Optional[str] = "",
        theme_color: Optional[str] = "",
        facts: Optional[list] = None,
        action_button_name: Optional[str] = "",
        action_button_url: Optional[str] = "",
        icon_url: Optional[str] = "",
        proxy: Optional[str] = "",
    ):
        super().__init__(http_conn_id=http_conn_id)
        self.webhook_token = self._get_token(webhook_token, http_conn_id)
        self.message = message
        self.subtitle = subtitle
        self.theme_color = theme_color
        self.facts = facts
        self.action_button_name = action_button_name
        self.action_button_url = action_button_url
        self.icon_url = icon_url
        self.proxy = proxy

    def _get_token(self, token: Optional[str], http_conn_id: Optional[str]) -> str:
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
            password = getattr(conn, 'password', '')
            return password
        else:
            raise AirflowException('Cannot get token: No valid Teams webhook token nor conn_id supplied')

    def _build_teams_message(self) -> str:
        """
        Construct Teams connector message. All the relevant parameters are combined to a valid
        Teams json message.

        :return: JSON formatted MS Teams connector message to send
        :rtype: str
        """
        card = {
            "themeColor": self.theme_color,
            "summary": self.message,
            "sections": [
                {
                    "activityTitle": self.message,
                    "activitySubtitle": self.subtitle,
                    "activityImage": self.icon_url,
                    "facts": self.facts,
                    "potentialAction": [
                        {
                            "@context": "http://schema.org",
                            "@type": "OpenUri",
                            "name": self.action_button_name,
                            "targets": [{"os": "default", "uri": self.action_button_url}],
                        }
                    ],
                }
            ],
        }
        return json.dumps(card)

    def send(self) -> None:
        """Remote Popen (actually execute the MS Teams webhook call)"""
        proxies = {}
        if self.proxy:
            proxies = {'https': self.proxy}

        teams_message = self._build_teams_message()
        self.run(
            endpoint=self.webhook_token,
            data=teams_message,
            headers={'Content-type': 'application/json'},
            extra_options={'proxies': proxies, 'check_response': True},
        )
