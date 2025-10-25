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
"""Operator for Telegram."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.telegram.hooks.telegram import TelegramHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class TelegramOperator(BaseOperator):
    """
    This operator allows you to post messages to Telegram using Telegram Bot API.

    Takes both Telegram Bot API token directly or connection that has Telegram token in password field.
    If both supplied, token parameter will be given precedence.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TelegramOperator`

    :param telegram_conn_id: Telegram connection ID which its password is Telegram API token
    :param token: Telegram API Token
    :param chat_id: Telegram chat ID for a chat/channel/group
    :param text: Message to be sent on telegram
    :param telegram_kwargs: Extra args to be passed to telegram client
    """

    template_fields: Sequence[str] = ("text", "chat_id")
    ui_color = "#FFBA40"

    def __init__(
        self,
        *,
        telegram_conn_id: str = "telegram_default",
        token: str | None = None,
        chat_id: str | None = None,
        text: str = "No message has been set.",
        telegram_kwargs: dict | None = None,
        **kwargs,
    ):
        self.chat_id = chat_id
        self.token = token
        self.telegram_kwargs = telegram_kwargs or {}
        self.text = text

        if telegram_conn_id is None:
            raise AirflowException("No valid Telegram connection id supplied.")

        self.telegram_conn_id = telegram_conn_id

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Call the TelegramHook to post the provided Telegram message."""
        if self.text:
            self.telegram_kwargs["text"] = self.text

        telegram_hook = TelegramHook(
            telegram_conn_id=self.telegram_conn_id,
            token=self.token,
            chat_id=self.chat_id,
        )
        telegram_hook.send_message(self.telegram_kwargs)


class TelegramFileOperator(BaseOperator):
    """
    This operator allows you to send file to Telegram using Telegram Bot API.

    Takes both Telegram Bot API token directly or connection that has Telegram token in password field.
    If both supplied, token parameter will be given precedence.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TelegramOperator`

    :param telegram_conn_id: Telegram connection ID which its password is Telegram API token
    :param token: Telegram API Token
    :param chat_id: Telegram chat ID for a chat/channel/group
    :param file: The path of the file or media to be sent via Telegram
    :param telegram_kwargs: Extra args to be passed to telegram client
    """

    template_fields: Sequence[str] = "chat_id"
    ui_color = "#FFBA40"

    def __init__(
        self,
        *,
        telegram_conn_id: str = "telegram_default",
        token: str | None = None,
        chat_id: str | None = None,
        file: str,
        telegram_kwargs: dict | None = None,
        **kwargs,
    ):
        self.chat_id = chat_id
        self.token = token
        self.telegram_kwargs = telegram_kwargs or {}
        self.file = file

        if telegram_conn_id is None:
            raise AirflowException("No valid Telegram connection id supplied.")

        self.telegram_conn_id = telegram_conn_id

        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Call the TelegramHook to send the provided Telegram file."""
        if self.file:
            self.telegram_kwargs["file"] = self.file

        telegram_hook = TelegramHook(
            telegram_conn_id=self.telegram_conn_id,
            token=self.token,
            chat_id=self.chat_id,
        )
        telegram_hook.send_file(**self.telegram_kwargs)
