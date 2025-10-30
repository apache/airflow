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
import tenacity

import airflow
from airflow.models import Connection
from airflow.providers.telegram.hooks.telegram import TelegramHook

TELEGRAM_TOKEN = "dummy token"


class AsyncMock(mock.MagicMock):
    async def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


def telegram_error_side_effect(*args, **kwargs):
    raise telegram.error.TelegramError("cosmic rays caused bit flips")


class TestTelegramHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="telegram-webhook-without-token",
                conn_type="http",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="telegram_default",
                conn_type="http",
                password=TELEGRAM_TOKEN,
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="telegram-webhook-with-chat_id",
                conn_type="http",
                password=TELEGRAM_TOKEN,
                host="-420913222",
            )
        )

    def test_should_use_default_connection(self):
        hook = TelegramHook()

        assert hook.token == TELEGRAM_TOKEN
        assert not hook.chat_id

    @pytest.mark.db_test
    def test_should_raise_exception_if_conn_id_doesnt_exist(self, sdk_connection_not_found):
        with pytest.raises(airflow.exceptions.AirflowNotFoundException) as ctx:
            TelegramHook(telegram_conn_id="telegram-webhook-non-existent")

        assert str(ctx.value) == "The conn_id `telegram-webhook-non-existent` isn't defined"

    def test_should_raise_exception_if_conn_id_doesnt_contain_token(self):
        with pytest.raises(airflow.exceptions.AirflowException) as ctx:
            TelegramHook(telegram_conn_id="telegram-webhook-without-token")

        assert str(ctx.value) == "Missing token(password) in Telegram connection"

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_raise_exception_if_chat_id_is_not_provided_anywhere(self, mock_get_conn):
        hook = TelegramHook(telegram_conn_id="telegram_default")
        error_message = "'chat_id' must be provided for telegram message"
        with pytest.raises(airflow.exceptions.AirflowException, match=error_message):
            hook.send_message({"text": "test telegram message"})

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_raise_exception_if_message_text_is_not_provided(self, mock_get_conn):
        hook = TelegramHook(telegram_conn_id="telegram_default")
        error_message = "'text' must be provided for telegram message"
        with pytest.raises(airflow.exceptions.AirflowException, match=error_message):
            hook.send_message({"chat_id": "-420913222"})

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_message_if_all_parameters_are_correctly_provided(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default")
        hook.send_message({"chat_id": "-420913222", "text": "test telegram message"})

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
    def test_should_send_message_if_chat_id_is_provided_through_constructor(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default", chat_id="-420913222")
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
    def test_should_send_message_if_chat_id_is_provided_in_connection(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

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
        mock_get_conn.return_value = AsyncMock(password="some_token")

        mock_get_conn.return_value.send_message.side_effect = telegram_error_side_effect

        hook = TelegramHook(telegram_conn_id="telegram-webhook-with-chat_id")
        with pytest.raises(tenacity.RetryError) as ctx:
            hook.send_message({"text": "test telegram message"})
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
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(token=TELEGRAM_TOKEN, chat_id="-420913222")
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
    def test_should_raise_exception_if_chat_id_is_not_provided_anywhere_when_sending_file(
        self, mock_get_conn
    ):
        hook = TelegramHook(telegram_conn_id="telegram_default")
        error_message = "'chat_id' must be provided for telegram document message"
        with pytest.raises(airflow.exceptions.AirflowException, match=error_message):
            hook.send_file({"file": "/file/to/path"})

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_raise_exception_if_file_path_is_not_provided_when_sending_file(self, mock_get_conn):
        hook = TelegramHook(telegram_conn_id="telegram_default")
        error_message = "'file' parameter must be provided for sending a Telegram document message"
        with pytest.raises(airflow.exceptions.AirflowException, match=error_message):
            hook.send_file({"chat_id": "-420913222"})

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_file_if_all_parameters_are_correctly_provided(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default")
        hook.send_file({"chat_id": "-420913222", "file": "/file/to/path"})

        mock_get_conn.return_value.send_document.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_document.assert_called_once_with(
            **{
                "chat_id": "-420913222",
                "document": "/file/to/path",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_file_if_chat_id_is_provided_through_constructor(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram_default", chat_id="-420913222")
        hook.send_file({"file": "/file/to/path"})

        mock_get_conn.return_value.send_document.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_document.assert_called_once_with(
            **{
                "chat_id": "-420913222",
                "document": "/file/to/path",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_file_if_chat_id_is_provided_in_connection(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(telegram_conn_id="telegram-webhook-with-chat_id")
        hook.send_file({"file": "/file/to/path"})

        mock_get_conn.return_value.send_document.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_document.assert_called_once_with(
            **{
                "chat_id": "-420913222",
                "document": "/file/to/path",
            }
        )

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_retry_on_telegram_error_when_sending_file(self, mock_get_conn):
        excepted_retry_count = 5
        mock_get_conn.return_value = AsyncMock(password="some_token")

        mock_get_conn.return_value.send_document.side_effect = telegram_error_side_effect

        hook = TelegramHook(telegram_conn_id="telegram-webhook-with-chat_id")
        with pytest.raises(tenacity.RetryError) as ctx:
            hook.send_file({"file": "/file/to/path"})
        assert "state=finished raised TelegramError" in str(ctx.value)

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_document.assert_called_with(
            **{
                "chat_id": "-420913222",
                "document": "/file/to/path",
            }
        )
        assert excepted_retry_count == mock_get_conn.return_value.send_document.call_count

    @mock.patch("airflow.providers.telegram.hooks.telegram.TelegramHook.get_conn")
    def test_should_send_file_if_token_is_provided(self, mock_get_conn):
        mock_get_conn.return_value = AsyncMock(password="some_token")

        hook = TelegramHook(token=TELEGRAM_TOKEN, chat_id="-420913222")
        hook.send_file({"file": "/file/to/path"})

        mock_get_conn.return_value.send_document.return_value = "OK."

        mock_get_conn.assert_called_once()
        mock_get_conn.return_value.send_document.assert_called_once_with(
            **{
                "chat_id": "-420913222",
                "document": "/file/to/path",
            }
        )
