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
from airflow.exceptions import AirflowException
from airflow.providers.ddiscord.hooks.discord_webhook import DiscordWebhookHook
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.decorators import apply_defaults


class DiscordWebhookOperator(SimpleHttpOperator):
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
    :type http_conn_id: str
    :param webhook_endpoint: Discord webhook endpoint in the form of
                             "webhooks/{webhook.id}/{webhook.token}"
    :type webhook_endpoint: str
    :param message: The message you want to send to your Discord channel
                    (max 2000 characters). (templated)
    :type message: str
    :param username: Override the default username of the webhook. (templated)
    :type username: str
    :param avatar_url: Override the default avatar of the webhook
    :type avatar_url: str
    :param tts: Is a text-to-speech message
    :type tts: bool
    :param proxy: Proxy to use to make the Discord webhook call
    :type proxy: str
    """

    template_fields = ['username', 'message']

    @apply_defaults
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
        super().__init__(endpoint=webhook_endpoint,
                         *args,
                         **kwargs)

        if not http_conn_id:
            raise AirflowException('No valid Discord http_conn_id supplied.')

        self.http_conn_id = http_conn_id
        self.webhook_endpoint = webhook_endpoint
        self.message = message
        self.username = username
        self.avatar_url = avatar_url
        self.tts = tts
        self.proxy = proxy
        self.hook = None

    def execute(self, context):
        """
        Call the DiscordWebhookHook to post message
        """
        self.hook = DiscordWebhookHook(
            self.http_conn_id,
            self.webhook_endpoint,
            self.message,
            self.username,
            self.avatar_url,
            self.tts,
            self.proxy
        )
        self.hook.execute()
