# -*- coding: utf-8 -*-
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
import re

from airflow.exceptions import AirflowException
from airflow.hooks.http_hook import HttpHook


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
    :type http_conn_id: str
    :param webhook_endpoint: Discord webhook endpoint in the form of
                             "webhooks/{webhook.id}/{webhook.token}"
    :type webhook_endpoint: str
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters)
    :type message: str
    :param username: Override the default username of the webhook
    :type username: str
    :param avatar_url: Override the default avatar of the webhook
    :type avatar_url: str
    :param tts: Is a text-to-speech message
    :type tts: bool
    :param proxy: Proxy to use to make the Discord webhook call
    :type proxy: str
    """

    def __init__(self,
                 http_conn_id=None,
                 webhook_endpoint=None,
                 message="",
                 username=None,
                 avatar_url=None,
                 tts=False,
                 proxy=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.webhook_endpoint = self._get_webhook_endpoint(http_conn_id, webhook_endpoint)
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy

    def _get_webhook_endpoint(self, http_conn_id, webhook_endpoint):
        """
        Given a Discord http_conn_id, return the default webhook endpoint or override if a
        webhook_endpoint is manually supplied.

        :param http_conn_id: The provided connection ID
        :param webhook_endpoint: The manually provided webhook endpoint
        :return: Webhook endpoint (str) to use
        """
        if webhook_endpoint:
            endpoint = webhook_endpoint
        elif http_conn_id:
            conn = self.get_connection(http_conn_id)
            extra = conn.extra_dejson
            endpoint = extra.get('webhook_endpoint', '')
        else:
            raise AirflowException('Cannot get webhook endpoint: No valid Discord '
                                   'webhook endpoint or http_conn_id supplied.')

        # make sure endpoint matches the expected Discord webhook format
        if not re.match('^webhooks/[0-9]+/[a-zA-Z0-9_-]+$', endpoint):
            raise AirflowException('Expected Discord webhook endpoint in the form '
                                   'of "webhooks/{webhook.id}/{webhook.token}".')

        return endpoint

    def _build_discord_payload(self):
        """
        Construct the Discord JSON payload. All relevant parameters are combined here
        to a valid Discord JSON payload.

        :return: Discord payload (str) to send
        """
        payload = {}

        if self.username:
            payload['username'] = self.username
        if self.avatar_url:
            payload['avatar_url'] = self.avatar_url

        payload['tts'] = self.tts

        if len(self.message) <= 2000:
            payload['content'] = self.message
        else:
            raise AirflowException('Discord message length must be 2000 or fewer '
                                   'characters.')

        return json.dumps(payload)

    def execute(self):
        """
        Execute the Discord webhook call
        """
        proxies = {}
        if self.proxy:
            # we only need https proxy for Discord
            proxies = {'https': self.proxy}

        discord_payload = self._build_discord_payload()

        self.run(endpoint=self.webhook_endpoint,
                 data=discord_payload,
                 headers={'Content-type': 'application/json'},
                 extra_options={'proxies': proxies})
