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
"""Hook for Telegram."""

from __future__ import annotations

import asyncio
from typing import Any

import telegram
import tenacity

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook


class TelegramHook(BaseHook):
    """
    This hook allows you to post messages to Telegram using the telegram python-telegram-bot library.

    The library can be found here: https://github.com/python-telegram-bot/python-telegram-bot
    It accepts both telegram bot API token directly or connection that has telegram bot API token.
    If both supplied, token parameter will be given precedence, otherwise 'password' field in the connection
    from telegram_conn_id will be used.
    chat_id can also be provided in the connection using 'host' field in connection.
    Following is the details of a telegram_connection:
    name: 'telegram-connection-name'
    conn_type: 'telegram'
    password: 'TELEGRAM_TOKEN' (optional)
    host: 'chat_id' (optional)
    Examples:
    .. code-block:: python

        # Create hook
        telegram_hook = TelegramHook(telegram_conn_id="telegram_default")
        telegram_hook = TelegramHook()  # will use telegram_default
        # or telegram_hook = TelegramHook(telegram_conn_id='telegram_default', chat_id='-1xxx')
        # or telegram_hook = TelegramHook(token='xxx:xxx', chat_id='-1xxx')

        # Call method from telegram bot client
        telegram_hook.send_message(None, {"text": "message", "chat_id": "-1xxx"})
        # or telegram_hook.send_message(None', {"text": "message"})

    :param telegram_conn_id: connection that optionally has Telegram API token in the password field
    :param token: optional telegram API token
    :param chat_id: optional chat_id of the telegram chat/channel/group
    """

    conn_name_attr = "telegram_conn_id"
    default_conn_name = "telegram_default"
    conn_type = "telegram"
    hook_name = "Telegram"

    def __init__(
        self,
        telegram_conn_id: str | None = default_conn_name,
        token: str | None = None,
        chat_id: str | None = None,
    ) -> None:
        super().__init__()
        self.token = self.__get_token(token, telegram_conn_id)
        self.chat_id = self.__get_chat_id(chat_id, telegram_conn_id)
        self.connection = self.get_conn()

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "extra", "login", "port", "extra"],
            "relabeling": {},
        }

    def get_conn(self) -> telegram.Bot:
        """
        Return the telegram bot client.

        :return: telegram bot client
        """
        return telegram.Bot(self.token)

    def __get_token(self, token: str | None, telegram_conn_id: str | None) -> str:
        """
        Return the telegram API token.

        :param token: telegram API token
        :param telegram_conn_id: telegram connection name
        :return: telegram API token
        """
        if token is not None:
            return token

        if telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not conn.password:
                raise AirflowException("Missing token(password) in Telegram connection")

            return conn.password

        raise AirflowException("Cannot get token: No valid Telegram connection supplied.")

    def __get_chat_id(self, chat_id: str | None, telegram_conn_id: str | None) -> str | None:
        """
        Return the telegram chat ID for a chat/channel/group.

        :param chat_id: optional chat ID
        :param telegram_conn_id: telegram connection name
        :return: telegram chat ID
        """
        if chat_id is not None:
            return chat_id

        if telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)
            return conn.host

        return None

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(telegram.error.TelegramError),
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(1),
    )
    def send_message(self, api_params: dict) -> None:
        """
        Send the message to a telegram channel or chat.

        :param api_params: params for telegram_instance.send_message. It can also be used to override chat_id
        """
        kwargs: dict[str, Any] = {
            "parse_mode": telegram.constants.ParseMode.HTML,
            "disable_web_page_preview": True,
        }
        if self.chat_id is not None:
            kwargs["chat_id"] = self.chat_id
        kwargs.update(api_params)

        if "text" not in kwargs or kwargs["text"] is None:
            raise AirflowException("'text' must be provided for telegram message")

        if kwargs.get("chat_id") is None:
            raise AirflowException("'chat_id' must be provided for telegram message")

        response = asyncio.run(self.connection.send_message(**kwargs))
        self.log.debug(response)

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(telegram.error.TelegramError),
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(1),
    )
    def send_file(self, api_params: dict) -> None:
        """
        Send the file to a telegram channel or chat.

        :param api_params: params for telegram_instance.send_document. It can also be used to override chat_id
        """
        kwargs: dict[str, Any] = {}

        if self.chat_id is not None:
            kwargs["chat_id"] = self.chat_id
        kwargs.update(api_params)

        if "file" not in kwargs or kwargs["file"] is None:
            raise AirflowException(
                "'file' parameter must be provided for sending a Telegram document message"
            )

        kwargs["document"] = kwargs.pop("file")  # rename 'file' to 'document'

        if kwargs.get("chat_id") is None:
            raise AirflowException("'chat_id' must be provided for telegram document message")

        response = asyncio.run(self.connection.send_document(**kwargs))
        self.log.debug(response)
