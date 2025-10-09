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
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook


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
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.http_conn_id: Any = http_conn_id
        self.webhook_endpoint = self._get_webhook_endpoint(http_conn_id, webhook_endpoint)
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy

    def _get_webhook_endpoint(self, http_conn_id: str | None, webhook_endpoint: str | None) -> str:
        """
        Return the default webhook endpoint or override if a webhook_endpoint is manually supplied.

        :param http_conn_id: The provided connection ID
        :param webhook_endpoint: The manually provided webhook endpoint
        :return: Webhook endpoint (str) to use
        """
        if webhook_endpoint:
            endpoint = webhook_endpoint
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            endpoint = extra.get("webhook_endpoint", "")
        else:
            raise AirflowException(
                "Cannot get webhook endpoint: No valid Discord webhook endpoint or http_conn_id supplied."
            )

        # make sure endpoint matches the expected Discord webhook format
        if not re.fullmatch("webhooks/[0-9]+/[a-zA-Z0-9_-]+", endpoint):
            raise AirflowException(
                'Expected Discord webhook endpoint in the form of "webhooks/{webhook.id}/{webhook.token}".'
            )

        return endpoint

    def _build_discord_payload(self) -> str:
        """
        Combine all relevant parameters into a valid Discord JSON payload.

        :return: Discord payload (str) to send
        """
        payload: dict[str, Any] = {}

        if self.username:
            payload["username"] = self.username
        if self.avatar_url:
            payload["avatar_url"] = self.avatar_url

        payload["tts"] = self.tts

        if len(self.message) <= 2000:
            payload["content"] = self.message
        else:
            raise AirflowException("Discord message length must be 2000 or fewer characters.")

        return json.dumps(payload)

    def execute(self) -> None:
        """Execute the Discord webhook call."""
        proxies = {}
        if self.proxy:
            # we only need https proxy for Discord
            proxies = {"https": self.proxy}

        discord_payload = self._build_discord_payload()

        self.run(
            endpoint=self.webhook_endpoint,
            data=discord_payload,
            headers={"Content-type": "application/json"},
            extra_options={"proxies": proxies},
        )
