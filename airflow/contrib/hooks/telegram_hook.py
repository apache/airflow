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
import telegram
import tenacity

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class TelegramHook(BaseHook):
    """
       Interact with Telegram, using python-telegram-bot library.
    """

    def __init__(self, telegram_conn_id=None, chat_id=None):
        """
        Takes both telegram bot API token directly and connection that has telegram bot API token.
        If both supplied, telegram API token will be used.

        :param telegram_conn_id: connection that has telegram API token in the password field
        :type telegram_conn_id: str
        :param chat_id: Telegram public or private channel id (optional).
        Check https://stackoverflow.com/a/33862907 to see how you can obtain chat_id for private
        channel
        :type chat_id: str
        """
        self.token = self.__get_token(telegram_conn_id)
        self.chat_id = self.__get_chat_id(chat_id, telegram_conn_id)
        self.connection = self.get_conn()

    def get_conn(self):
        return telegram.Bot(token=self.token)

    def __get_token(self, telegram_conn_id):
        if telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not conn.password:
                raise AirflowException("Missing token(password) in Telegram connection")
            return conn.password
        else:
            raise AirflowException(
                "Cannot get token: " "No valid Telegram connection supplied."
            )

    def __get_chat_id(self, chat_id, telegram_conn_id):
        if chat_id is not None:
            return chat_id
        elif telegram_conn_id is not None:
            conn = self.get_connection(telegram_conn_id)

            if not getattr(conn, "host", None):
                raise AirflowException("Missing chat_id (host) in Telegram connection")
            return conn.host
        else:
            raise AirflowException(
                "Cannot get chat_id: " "No valid chat_id nor telegram_conn_id supplied."
            )

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(telegram.error.TelegramError),
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_fixed(1),
    )
    def call(self, method, api_params):
        """
        Send a message to a telegram channel

        :param method: not used
        :type method: str
        :param api_params: params for telegram_instance.send_message. You can use it also to override chat_id
        :type api_params: dict
        """

        params = {
            "chat_id": self.chat_id,
            "parse_mode": telegram.ParseMode.HTML,
            "disable_web_page_preview": True,
        }
        params.update(api_params)
        self.log.debug(self.connection.send_message(**params))


__all__ = ["TelegramHook"]
