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

from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.http.operators.http import HttpOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DiscordWebhookOperator(HttpOperator):
    """
    This operator allows you to post messages to Discord using incoming webhooks.

    Takes a Discord connection ID with a default relative webhook endpoint. The
    default endpoint can be overridden using the webhook_endpoint parameter
    (https://discordapp.com/developers/docs/resources/webhook).

    Each Discord webhook can be pre-configured to use a specific username and
    avatar_url. You can override these defaults in this operator.

    :param http_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
    :param webhook_endpoint: Discord webhook endpoint in the form of
                             "webhooks/{webhook.id}/{webhook.token}" (templated)
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters). (templated)
    :param username: Override the default username of the webhook. (templated)
    :param avatar_url: Override the default avatar of the webhook
    :param tts: Is a text-to-speech message
    :param proxy: Proxy to use to make the Discord webhook call
    """

    template_fields: Sequence[str] = ("username", "message", "webhook_endpoint")

    def __init__(
        self,
        *,
        http_conn_id: str | None = None,
        webhook_endpoint: str | None = None,
        message: str = "",
        username: str | None = None,
        avatar_url: str | None = None,
        tts: bool = False,
        proxy: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(endpoint=webhook_endpoint, **kwargs)

        if not http_conn_id:
            raise AirflowException("No valid Discord http_conn_id supplied.")

        self.http_conn_id = http_conn_id
        self.webhook_endpoint = webhook_endpoint
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy

    @property
    def hook(self) -> DiscordWebhookHook:
        hook = DiscordWebhookHook(
            self.http_conn_id,
            self.webhook_endpoint,
            self.message,
            self.username,
            self.avatar_url,
            self.tts,
            self.proxy,
        )
        return hook

    def execute(self, context: Context) -> None:
        """Call the DiscordWebhookHook to post a message."""
        self.hook.execute()
