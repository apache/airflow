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

from unittest import mock

import pytest
import telegram

import airflow
from airflow.models import Connection
from airflow.providers.telegram.hooks.telegram import TelegramHook
from airflow.utils import db

TELEGRAM_TOKEN = "dummy token"


class TestTelegramHook:
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="telegram-webhook-without-token",
                conn_type="http",
            )
        )
        db.merge_conn(
            Connection(
                conn_id="telegram_default",
                conn_type="http",
                password=TELEGRAM_TOKEN,
            )
        )
        db.merge_conn(
            Connection(
                conn_id="telegram-webhook-with-chat_id",
                conn_type="http",
                password=TELEGRAM_TOKEN,
                host="-420913222",
            )
        )

    def test_should_raise_exception_if_both_connection_or_token_is_not_provided(self):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            TelegramHook()

        assert "Cannot get token: No valid Telegram connection supplied." == str(ctx.value)

    def test_should_raise_exception_if_conn_id_doesnt_exist(self):
        with pytest.raises(airflow.exceptions.AirflowNotFoundException) as ctx:
            TelegramHook(telegram_conn_id="telegram-webhook-non-existent")

        assert "The conn_id `telegram-webhook-non-existent` isn't defined" == str(ctx.value)

    def test_should_raise_exception_if_conn_id_doesnt_contain_token(self):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            TelegramHook(telegram_conn_id="telegram-webhook-without-token")

        assert "Missing token(password) in Telegram connection" == str(ctx.value)

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_raise_exception_if_chat_id_is_not_provided_anywhere(self, mock_get_conn):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            hook = TelegramHook(telegram_conn_id="telegram_default")
            hook.send_message({"text": "test telegram message"})

        assert "'chat_id' must be provided for telegram message" == str(ctx.value)

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_raise_exception_if_message_text_is_not_provided(self, mock_get_conn):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            hook = TelegramHook(telegram_conn_id="telegram_default")
            hook.send_message({"chat_id": -420913222})

        assert "'text' must be provided for telegram message" == str(ctx.value)

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_message_if_all_parameters_are_correctly_provided(self, mock_get_conn):
        mock_get_conn.return_value = mock.Mock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default")
        hook.send_message({"chat_id": -420913222, "text": "test telegram message"})

        mock_get_conn.return_value.send_message.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_message.assert_called_once_with(
            **{
                "chat_id": -420913222,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "text": "test telegram message",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_message_if_chat_id_is_provided_through_constructor(self, mock_get_conn):
        mock_get_conn.return_value = mock.Mock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default", chat_id=-420913222)
        hook.send_message({"text": "test telegram message"})

        mock_get_conn.return_value.send_message.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_message.assert_called_once_with(
            **{
                "chat_id": -420913222,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "text": "test telegram message",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_message_if_chat_id_is_provided_in_connection(self, mock_get_conn):
        mock_get_conn.return_value = mock.Mock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram-webhook-with-chat_id")
        hook.send_message({"text": "test telegram message"})

        mock_get_conn.return_value.send_message.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_message.assert_called_once_with(
            **{
                "chat_id": "-420913222",
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "text": "test telegram message",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_retry_when_any_telegram_error_is_encountered(self, mock_get_conn):
        excepted_retry_count = 5
        mock_get_conn.return_value = mock.Mock(password="some_token")

        def side_effect(*args, **kwargs):
            raise telegram.error.TelegramError("cosmic rays caused bit flips")

        mock_get_conn.return_value.send_message.side_effect = side_effect

        with pytest.raises(Exception) as ctx:
            hook = TelegramHook(telegram_conn_id="telegram-webhook-with-chat_id")
            hook.send_message({"text": "test telegram message"})

        assert "RetryError" in str(ctx.value)
        assert "state=finished raised TelegramError" in str(ctx.value)

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_message.assert_called_with(
            **{
                "chat_id": "-420913222",
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "text": "test telegram message",
            }
        )
        assert excepted_retry_count == mock_get_conn.return_value.send_message.call_count

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_message_if_token_is_provided(self, mock_get_conn):
        mock_get_conn.return_value = mock.Mock(password="some_token")

        hook = TelegramHook(token=TELEGRAM_TOKEN, chat_id=-420913222)
        hook.send_message({"text": "test telegram message"})

        mock_get_conn.return_value.send_message.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_message.assert_called_once_with(
            **{
                "chat_id": -420913222,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
                "text": "test telegram message",
            }
        )
