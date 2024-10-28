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

from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.discord.hooks.discord_webhook import DiscordWebhookHook

ICON_URL: str = (
    "https://raw.githubusercontent.com/apache/airflow/main/airflow/www/static/pin_100.png"
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
    """

    # A property that specifies the attributes that can be templated.
    template_fields = ("discord_conn_id", "text", "username", "avatar_url", "tts")

    def __init__(
        self,
        discord_conn_id: str = "discord_webhook_default",
        text: str = "This is a default message",
        username: str = "Airflow",
        avatar_url: str = ICON_URL,
        tts: bool = False,
    ):
        super().__init__()
        self.discord_conn_id = discord_conn_id
        self.text = text
        self.username = username
        self.avatar_url = avatar_url

        # If you're having problems with tts not being recognized in __init__(),
        # you can define that after instantiating the class
        self.tts = tts

    @cached_property
    def hook(self) -> DiscordWebhookHook:
        """Discord Webhook Hook."""
        return DiscordWebhookHook(http_conn_id=self.discord_conn_id)

    def notify(self, context):
        """Send a message to a Discord channel."""
        self.hook.username = self.username
        self.hook.message = self.text
        self.hook.avatar_url = self.avatar_url
        self.hook.tts = self.tts

        self.hook.execute()
