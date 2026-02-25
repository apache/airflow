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

from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.common.compat.notifier import BaseNotifier
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookAsyncHook, DiscordWebhookHook
from airflow.providers.discord.version_compat import AIRFLOW_V_3_1_PLUS

if TYPE_CHECKING:
    from airflow.providers.discord.notifications.embed import Embed

ICON_URL: str = (
    "https://raw.githubusercontent.com/apache/airflow/main/airflow-core/src/airflow/ui/public/pin_100.png"
)


class DiscordNotifier(BaseNotifier):
    """
    Discord BaseNotifier.

    :param discord_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
    :param text: The content of the message
    :param username: The username to send the message as. Optional
    :param avatar_url: The URL of the avatar to use for the message. Optional
    :param tts: Text to speech.
    :param embed: Discord embed object. See:
           https://discord.com/developers/docs/resources/message#embed-object-embed-author-structure
    """

    # A property that specifies the attributes that can be templated.
    template_fields = ("discord_conn_id", "text", "username", "avatar_url", "tts", "embed")

    def __init__(
        self,
        discord_conn_id: str = "discord_webhook_default",
        text: str = "",
        username: str = "Airflow",
        avatar_url: str = ICON_URL,
        tts: bool = False,
        embed: Embed | None = None,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.discord_conn_id = discord_conn_id
        self.text = text
        self.username = username
        self.avatar_url = avatar_url

        # If you're having problems with tts not being recognized in __init__(),
        # you can define that after instantiating the class
        self.tts = tts
        self.embed = embed

    @cached_property
    def hook(self) -> DiscordWebhookHook:
        """Discord Webhook Hook."""
        return DiscordWebhookHook(http_conn_id=self.discord_conn_id)

    @cached_property
    def hook_async(self) -> DiscordWebhookAsyncHook:
        """Discord Webhook Async Hook."""
        return DiscordWebhookAsyncHook(
            http_conn_id=self.discord_conn_id,
            message=self.text,
            username=self.username,
            avatar_url=self.avatar_url,
            tts=self.tts,
            embed=self.embed,
        )

    def notify(self, context):
        """
        Send a message to a Discord channel.

        :param context: the context object
        :return: None
        """
        self.hook.username = self.username
        self.hook.message = self.text
        self.hook.avatar_url = self.avatar_url
        self.hook.tts = self.tts
        self.hook.embed = self.embed

        self.hook.execute()

    async def async_notify(self, context) -> None:
        """
        Send a message to a Discord channel using async HTTP.

        :param context: the context object
        :return: None
        """
        await self.hook_async.execute()
