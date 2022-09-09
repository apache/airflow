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
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.microsoft.teams.hooks.teams_webhook import TeamsWebhookHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TeamsWebhookOperator(SimpleHttpOperator):
    """
    This operator allows you to post messages to MS Teams using the incoming webhooks.
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

    template_fields: Sequence[str] = (
        'message',
        'subtitle',
        'theme_color',
        'proxy',
    )

    def __init__(
        self,
        http_conn_id: str,
        webhook_token: Optional[str] = None,
        message: str = "",
        subtitle: Optional[str] = None,
        theme_color: Optional[str] = None,
        facts: Optional[list] = None,
        action_button_name: Optional[str] = None,
        action_button_url: Optional[str] = None,
        icon_url: Optional[str] = None,
        proxy: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(endpoint=webhook_token, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_token = webhook_token
        self.message = message
        self.subtitle = subtitle
        self.theme_color = theme_color
        self.facts = facts
        self.action_button_name = action_button_name
        self.action_button_url = action_button_url
        self.icon_url = icon_url
        self.proxy = proxy

    def execute(self, context: 'Context') -> None:
        hook = TeamsWebhookHook(
            self.http_conn_id,
            self.webhook_token,
            self.message,
            self.subtitle,
            self.theme_color,
            self.facts,
            self.action_button_name,
            self.action_button_url,
            self.icon_url,
            self.proxy,
        )
        hook.send()
