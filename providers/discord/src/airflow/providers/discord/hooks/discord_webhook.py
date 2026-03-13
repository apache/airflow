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
import re
from typing import TYPE_CHECKING, Any

import aiohttp

from airflow.providers.common.compat.connection import get_async_connection
from airflow.providers.http.hooks.http import HttpAsyncHook, HttpHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection
    from airflow.providers.discord.notifications.embed import Embed


class DiscordCommonHandler:
    """Contains the common functionality."""

    def get_webhook_endpoint(self, conn: Connection | None, webhook_endpoint: str | None) -> str:
        """
        Return the default webhook endpoint or override if a webhook_endpoint is manually supplied.

        :param conn: Airflow Discord connection
        :param webhook_endpoint: The manually provided webhook endpoint
        :return: Webhook endpoint (str) to use
        """
        if webhook_endpoint:
            endpoint = webhook_endpoint
        elif conn:
            extra = conn.extra_dejson
            endpoint = extra.get("webhook_endpoint", "")
        else:
            raise ValueError(
                "Cannot get webhook endpoint: No valid Discord webhook endpoint or http_conn_id supplied."
            )

        # make sure endpoint matches the expected Discord webhook format
        if not re.fullmatch("webhooks/[0-9]+/[a-zA-Z0-9_-]+", endpoint):
            raise ValueError(
                'Expected Discord webhook endpoint in the form of "webhooks/{webhook.id}/{webhook.token}".'
            )

        return endpoint

    def validate_embed(
        self,
        *,
        embed: Embed,
    ) -> Embed:
        """
        Validate Discord Embed JSON payload.

        Validates the embed object against Discord limits. See:
            https://discord.com/developers/docs/resources/message#embed-object-embed-limits

        :param embed: Discord embed object.
        :return: Discord embed object.
        """
        # Validate title
        if "title" in embed and len(embed["title"]) > 256:
            raise ValueError("Discord embed title must be 256 or fewer characters")
        # Validate description
        if "description" in embed and len(embed["description"]) > 4096:
            raise ValueError("Discord embed description must be 4096 or fewer characters")
        # Validate footer
        if "footer" in embed:
            if len(embed["footer"]["text"]) > 2048:
                raise ValueError("Discord embed footer text must be 2048 or fewer characters")
        # Validate author
        if "author" in embed:
            if len(embed["author"]["name"]) > 2048:
                raise ValueError("Discord embed author name must be 256 or fewer characters")
        # Validate fields
        if "fields" in embed:
            if len(embed["fields"]) > 25:
                raise ValueError("Discord embed fields must be 25 or fewer items")
            for field in embed["fields"]:
                if len(field["name"]) > 256:
                    raise ValueError("Discord embed field name must be 256 or fewer characters")
                if len(field["value"]) > 1024:
                    raise ValueError("Discord embed field value must be 1024 or fewer characters")
        return embed

    def build_discord_payload(
        self,
        *,
        tts: bool,
        message: str,
        username: str | None,
        avatar_url: str | None,
        embed: Embed | None = None,
    ) -> str:
        """
        Build a valid Discord JSON payload.

        :param tts: Is a text-to-speech message
        :param message: The message you want to send to your Discord channel
                        (max 2000 characters)
        :param username: Override the default username of the webhook
        :param avatar_url: Override the default avatar of the webhook
        :param embed: Discord embed object.
        :return: Discord payload (str) to send
        """
        if len(message) > 2000:
            raise ValueError("Discord message length must be 2000 or fewer characters.")
        payload: dict[str, Any] = {
            "content": message,
            "tts": tts,
        }
        if username:
            payload["username"] = username
        if avatar_url:
            payload["avatar_url"] = avatar_url
        if embed:
            payload["embeds"] = [self.validate_embed(embed=embed)]
        return json.dumps(payload)


class DiscordWebhookHook(HttpHook):
    """
    This hook allows you to post messages to Discord using incoming webhooks.

    Takes a Discord connection ID with a default relative webhook endpoint. The
    default endpoint can be overridden using the webhook_endpoint parameter
    (https://discordapp.com/developers/docs/resources/webhook).

    Each Discord webhook can be pre-configured to use a specific username and
    avatar_url. You can override these defaults in this hook.

    :param http_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
    :param webhook_endpoint: Discord webhook endpoint in the form of
                             "webhooks/{webhook.id}/{webhook.token}"
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters)
    :param username: Override the default username of the webhook
    :param avatar_url: Override the default avatar of the webhook
    :param tts: Is a text-to-speech message
    :param proxy: Proxy to use to make the Discord webhook call
    :param embed: Discord embed object.
    """

    conn_name_attr = "http_conn_id"
    default_conn_name = "discord_default"
    conn_type = "discord"
    hook_name = "Discord"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Discord connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField
        from wtforms.validators import Optional

        return {
            "webhook_endpoint": StringField(
                lazy_gettext("Webhook Endpoint"),
                widget=BS3TextFieldWidget(),
                validators=[Optional()],
                default=None,
            ),
        }

    def __init__(
        self,
        http_conn_id: str | None = None,
        webhook_endpoint: str | None = None,
        message: str = "",
        username: str | None = None,
        avatar_url: str | None = None,
        tts: bool = False,
        proxy: str | None = None,
        embed: Embed | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.handler = DiscordCommonHandler()
        self.http_conn_id: Any = http_conn_id
        self.webhook_endpoint = self._get_webhook_endpoint(http_conn_id, webhook_endpoint)
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy
        self.embed = embed

    def _get_webhook_endpoint(self, http_conn_id: str | None, webhook_endpoint: str | None) -> str:
        """
        Return the default webhook endpoint or override if a webhook_endpoint is manually supplied.

        :param http_conn_id: The provided connection ID
        :param webhook_endpoint: The manually provided webhook endpoint
        :return: Webhook endpoint (str) to use
        """
        conn = None
        if not webhook_endpoint and http_conn_id:
            conn = self.get_connection(http_conn_id)
        return self.handler.get_webhook_endpoint(conn, webhook_endpoint)

    def execute(self) -> None:
        """Execute the Discord webhook call."""
        proxies = {}
        if self.proxy:
            # we only need https proxy for Discord
            proxies = {"https": self.proxy}

        discord_payload = self.handler.build_discord_payload(
            tts=self.tts,
            message=self.message,
            username=self.username,
            avatar_url=self.avatar_url,
            embed=self.embed,
        )

        self.run(
            endpoint=self.webhook_endpoint,
            data=discord_payload,
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )


class DiscordWebhookAsyncHook(HttpAsyncHook):
    """
    This hook allows you to post messages to Discord using incoming webhooks using async HTTP.

    Takes a Discord connection ID with a default relative webhook endpoint. The
    default endpoint can be overridden using the webhook_endpoint parameter
    (https://discordapp.com/developers/docs/resources/webhook).

    Each Discord webhook can be pre-configured to use a specific username and
    avatar_url. You can override these defaults in this hook.

    :param http_conn_id: Http connection ID with host as "https://discord.com/api/" and
                         default webhook endpoint in the extra field in the form of
                         {"webhook_endpoint": "webhooks/{webhook.id}/{webhook.token}"}
    :param webhook_endpoint: Discord webhook endpoint in the form of
                             "webhooks/{webhook.id}/{webhook.token}"
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters)
    :param username: Override the default username of the webhook
    :param avatar_url: Override the default avatar of the webhook
    :param tts: Is a text-to-speech message
    :param proxy: Proxy to use to make the Discord webhook call
    :param embed: Discord embed object.
    """

    default_headers = {
        "Content-Type": "application/json",
    }
    conn_name_attr = "http_conn_id"
    default_conn_name = "discord_default"
    conn_type = "discord"
    hook_name = "Async Discord"

    def __init__(
        self,
        *,
        http_conn_id: str = "",
        webhook_endpoint: str | None = None,
        message: str = "",
        username: str | None = None,
        avatar_url: str | None = None,
        tts: bool = False,
        proxy: str | None = None,
        embed: Embed | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_endpoint = webhook_endpoint
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy
        self.embed = embed
        self.handler = DiscordCommonHandler()

    async def _get_webhook_endpoint(self) -> str:
        """
        Return the default webhook endpoint or override if a webhook_endpoint is manually supplied.

        :param http_conn_id: The provided connection ID
        :param webhook_endpoint: The manually provided webhook endpoint
        :return: Webhook endpoint (str) to use
        """
        conn = None
        if not self.webhook_endpoint and self.http_conn_id:
            conn = await get_async_connection(self.http_conn_id)
        return self.handler.get_webhook_endpoint(conn, self.webhook_endpoint)

    async def execute(self) -> None:
        """Execute the Discord webhook call."""
        webhook_endpoint = await self._get_webhook_endpoint()
        discord_payload = self.handler.build_discord_payload(
            tts=self.tts,
            message=self.message,
            username=self.username,
            avatar_url=self.avatar_url,
            embed=self.embed,
        )
        async with aiohttp.ClientSession(proxy=self.proxy) as session:
            await super().run(
                session=session,
                endpoint=webhook_endpoint,
                data=discord_payload,
                headers=self.default_headers,
            )
