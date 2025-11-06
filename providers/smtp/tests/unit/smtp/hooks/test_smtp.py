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
import os
import smtplib
import tempfile
from email.mime.application import MIMEApplication
from unittest import mock
from unittest.mock import AsyncMock, Mock, call, patch

import aiosmtplib
import pytest

from airflow.exceptions import AirflowException
from airflow.providers.smtp.hooks.smtp import SmtpHook, build_xoauth2_string

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import Connection
else:
    from airflow.models.connection import Connection  # type: ignore[assignment]

smtplib_string = "airflow.providers.smtp.hooks.smtp.smtplib"

FROM_EMAIL = "from@example.com"
TO_EMAIL = "to@example.com"
TEST_EMAILS = [FROM_EMAIL, TO_EMAIL]

CONN_TYPE = "smtp"
SMTP_HOST = "smtp.example.com"
SMTP_LOGIN = "smtp_user"
SMTP_PASSWORD = "smtp_password"
ACCESS_TOKEN = "test-token"

CONN_ID_DEFAULT = "smtp_default"
CONN_ID_NONSSL = "smtp_nonssl"
CONN_ID_SSL_EXTRA = "smtp_ssl_extra"
CONN_ID_OAUTH = "smtp_oauth2"

DEFAULT_PORT = 465
NONSSL_PORT = 587

DEFAULT_TIMEOUT = 30
DEFAULT_RETRY_LIMIT = 5

TEST_SUBJECT = "test subject"
TEST_BODY = "<html>Test</html>"

SERVER_DISCONNECTED_ERROR = aiosmtplib.errors.SMTPServerDisconnected("Server disconnected")


def _create_fake_smtp(mock_smtplib, use_ssl=True):
    if use_ssl:
        mock_conn = Mock(spec=smtplib.SMTP_SSL)
        mock_smtplib.SMTP_SSL.return_value = mock_conn
    else:
        mock_conn = Mock(spec=smtplib.SMTP)
        mock_smtplib.SMTP.return_value = mock_conn

    mock_conn.close.return_value = ("OK", [])

    return mock_conn


class TestSmtpHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_DEFAULT,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=DEFAULT_PORT,
                extra=json.dumps(dict(from_email=FROM_EMAIL)),
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_NONSSL,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=NONSSL_PORT,
                extra=json.dumps(dict(disable_ssl=True, from_email=FROM_EMAIL)),
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_OAUTH,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=NONSSL_PORT,
                extra=json.dumps(dict(disable_ssl=True, from_email=FROM_EMAIL, access_token=ACCESS_TOKEN)),
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_SSL_EXTRA,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=None,
                password="None",
                port=DEFAULT_PORT,
                extra=json.dumps(dict(use_ssl=True, ssl_context="none", from_email=FROM_EMAIL)),
            )
        )

    @pytest.mark.parametrize(
        ("conn_id", "use_ssl", "expected_port", "create_context"),
        [
            pytest.param(CONN_ID_DEFAULT, True, DEFAULT_PORT, True, id="ssl-connection"),
            pytest.param(CONN_ID_NONSSL, False, NONSSL_PORT, False, id="non-ssl-connection"),
        ],
    )
    @patch(smtplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect(
        self, create_default_context, mock_smtplib, conn_id, use_ssl, expected_port, create_context
    ):
        """Test sync connection with different configurations."""
        mock_conn = _create_fake_smtp(mock_smtplib, use_ssl=use_ssl)

        with SmtpHook(smtp_conn_id=conn_id):
            pass

        if create_context:
            assert create_default_context.called
            mock_smtplib.SMTP_SSL.assert_called_once_with(
                host=SMTP_HOST,
                port=expected_port,
                timeout=DEFAULT_TIMEOUT,
                context=create_default_context.return_value,
            )
        else:
            mock_smtplib.SMTP.assert_called_once_with(
                host=SMTP_HOST,
                port=expected_port,
                timeout=DEFAULT_TIMEOUT,
            )

        mock_conn.login.assert_called_once_with(SMTP_LOGIN, SMTP_PASSWORD)
        assert mock_conn.close.call_count == 1

    @patch(smtplib_string)
    def test_get_email_address_single_email(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list(FROM_EMAIL) == [FROM_EMAIL]

    @pytest.mark.parametrize(
        "email_input",
        [
            pytest.param(f"{FROM_EMAIL}, {TO_EMAIL}", id="comma_separated"),
            pytest.param(f"{FROM_EMAIL}; {TO_EMAIL}", id="semicolon_separated"),
            pytest.param([FROM_EMAIL, TO_EMAIL], id="list_input"),
            pytest.param((FROM_EMAIL, TO_EMAIL), id="tuple_input"),
        ],
    )
    @patch(smtplib_string)
    def test_get_email_address_parsing(self, mock_smtplib, email_input):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list(email_input) == TEST_EMAILS

    @pytest.mark.parametrize(
        "invalid_input",
        [
            pytest.param(1, id="invalid_scalar_type"),
            pytest.param([FROM_EMAIL, 2], id="invalid_type_in_list"),
        ],
    )
    @patch(smtplib_string)
    def test_get_email_address_invalid_types(self, mock_smtplib, invalid_input):
        with pytest.raises(TypeError):
            with SmtpHook() as smtp_hook:
                smtp_hook._get_email_address_list(invalid_input)

    @patch(smtplib_string)
    def test_build_mime_message(self, mock_smtplib):
        mail_from = FROM_EMAIL
        mail_to = TO_EMAIL
        subject = TEST_SUBJECT
        html_content = TEST_BODY
        custom_headers = {"Reply-To": "reply_to@example.com"}
        with SmtpHook() as smtp_hook:
            msg, recipients = smtp_hook._build_mime_message(
                mail_from=mail_from,
                to=mail_to,
                subject=subject,
                html_content=html_content,
                custom_headers=custom_headers,
            )

        assert "From" in msg
        assert "To" in msg
        assert "Subject" in msg
        assert "Reply-To" in msg
        assert [mail_to] == recipients
        assert msg["To"] == ",".join(recipients)

    @patch(smtplib_string)
    def test_send_smtp(self, mock_smtplib):
        mock_send_mime = mock_smtplib.SMTP_SSL().sendmail
        with SmtpHook() as smtp_hook, tempfile.NamedTemporaryFile() as attachment:
            attachment.write(b"attachment")
            attachment.seek(0)
            smtp_hook.send_email_smtp(
                to=TO_EMAIL, subject=TEST_SUBJECT, html_content=TEST_BODY, files=[attachment.name]
            )
            assert mock_send_mime.called
            _, call_args = mock_send_mime.call_args
            assert call_args["from_addr"] == FROM_EMAIL
            assert call_args["to_addrs"] == [TO_EMAIL]
            msg = call_args["msg"]
            assert f"Subject: {TEST_SUBJECT}" in msg
            assert f"From: {FROM_EMAIL}" in msg
            filename = 'attachment; filename="' + os.path.basename(attachment.name) + '"'
            assert filename in msg
            mimeapp = MIMEApplication("attachment")
            assert mimeapp.get_payload() in msg

    @patch("airflow.providers.smtp.hooks.smtp.SmtpHook.get_connection")
    @patch(smtplib_string)
    def test_hook_conn(self, mock_smtplib, mock_hook_conn):
        mock_conn = Mock()
        mock_conn.login = SMTP_LOGIN
        mock_conn.password = SMTP_PASSWORD
        mock_conn.extra_dejson = {"disable_ssl": False}
        mock_hook_conn.return_value = mock_conn
        smtp_client_mock = mock_smtplib.SMTP_SSL()
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                from_email=FROM_EMAIL,
            )

            mock_hook_conn.assert_called_with(CONN_ID_DEFAULT)
            smtp_client_mock.login.assert_called_once_with(SMTP_LOGIN, SMTP_PASSWORD)
            smtp_client_mock.sendmail.assert_called_once()
        assert smtp_client_mock.close.called

    @pytest.mark.parametrize(
        ("conn_id", "ssl_context", "create_context_called", "use_default_context"),
        [
            pytest.param(CONN_ID_DEFAULT, "default", True, True, id="default_context"),
            pytest.param(CONN_ID_SSL_EXTRA, "none", False, False, id="none_context"),
            pytest.param(CONN_ID_DEFAULT, "default", True, True, id="explicit_default_context"),
        ],
    )
    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_context(
        self,
        create_default_context,
        mock_smtp,
        mock_smtp_ssl,
        conn_id,
        ssl_context,
        create_context_called,
        use_default_context,
    ):
        mock_smtp_ssl.return_value = Mock()

        with SmtpHook(conn_id) as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL, subject=TEST_SUBJECT, html_content=TEST_BODY, from_email=FROM_EMAIL
            )

        assert not mock_smtp.called
        if use_default_context:
            assert create_default_context.called
            expected_context = create_default_context.return_value
        else:
            create_default_context.assert_not_called()
            expected_context = None

        mock_smtp_ssl.assert_called_once_with(
            host=SMTP_HOST, port=DEFAULT_PORT, timeout=DEFAULT_TIMEOUT, context=expected_context
        )

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = Mock()

        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                from_email=FROM_EMAIL,
            )

        assert not mock_smtp.called
        assert create_default_context.called
        mock_smtp_ssl.assert_called_once_with(
            host=SMTP_HOST,
            port=DEFAULT_PORT,
            timeout=DEFAULT_TIMEOUT,
            context=create_default_context.return_value,
        )

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    def test_send_mime_nossl(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = Mock()

        with SmtpHook(smtp_conn_id=CONN_ID_NONSSL) as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL, subject=TEST_SUBJECT, html_content=TEST_BODY, from_email=FROM_EMAIL
            )

        assert not mock_smtp_ssl.called
        mock_smtp.assert_called_once_with(host=SMTP_HOST, port=NONSSL_PORT, timeout=DEFAULT_TIMEOUT)

    @patch("smtplib.SMTP")
    def test_send_mime_noauth(self, mock_smtp, create_connection_without_db):
        mock_smtp.return_value = Mock()
        conn = Connection(
            conn_id="smtp_noauth",
            conn_type=CONN_TYPE,
            host=SMTP_HOST,
            login=None,
            password="None",
            port=NONSSL_PORT,
            extra=json.dumps(dict(disable_ssl=True, from_email=FROM_EMAIL)),
        )
        create_connection_without_db(conn)
        with SmtpHook(smtp_conn_id="smtp_noauth") as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                from_email=FROM_EMAIL,
            )
        mock_smtp.assert_called_once_with(host=SMTP_HOST, port=NONSSL_PORT, timeout=DEFAULT_TIMEOUT)
        assert not mock_smtp.login.called

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                dryrun=True,
            )

        assert not mock_smtp.sendmail.called
        assert not mock_smtp_ssl.sendmail.called

    @patch("smtplib.SMTP_SSL")
    def test_send_mime_ssl_complete_failure(self, mock_smtp_ssl):
        mock_smtp_ssl().sendmail.side_effect = smtplib.SMTPServerDisconnected()

        with SmtpHook() as smtp_hook:
            with pytest.raises(smtplib.SMTPServerDisconnected):
                smtp_hook.send_email_smtp(
                    to=TO_EMAIL,
                    subject=TEST_SUBJECT,
                    html_content=TEST_BODY,
                )

        assert mock_smtp_ssl().sendmail.call_count == DEFAULT_RETRY_LIMIT

    @patch("email.message.Message.as_string")
    @patch("smtplib.SMTP_SSL")
    def test_send_mime_partial_failure(self, mock_smtp_ssl, mime_message_mock):
        mime_message_mock.return_value = "msg"
        final_mock = Mock()
        side_effects = [smtplib.SMTPServerDisconnected(), smtplib.SMTPServerDisconnected(), final_mock]
        mock_smtp_ssl.side_effect = side_effects
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
            )
        assert mock_smtp_ssl.call_count == side_effects.index(final_mock) + 1
        assert final_mock.starttls.called
        final_mock.sendmail.assert_called_once_with(from_addr=FROM_EMAIL, to_addrs=[TO_EMAIL], msg="msg")
        assert final_mock.close.called

    @patch("smtplib.SMTP_SSL")
    @patch("ssl.create_default_context")
    def test_send_mime_custom_timeout_retrylimit(
        self, create_default_context, mock_smtp_ssl, create_connection_without_db
    ):
        mock_smtp_ssl().sendmail.side_effect = smtplib.SMTPServerDisconnected()
        custom_retry_limit = 10
        custom_timeout = 60
        fake_conn = Connection(
            conn_id="mock_conn",
            conn_type=CONN_TYPE,
            host=SMTP_HOST,
            login=SMTP_LOGIN,
            password=SMTP_PASSWORD,
            port=DEFAULT_PORT,
            extra=json.dumps(
                dict(from_email=FROM_EMAIL, timeout=custom_timeout, retry_limit=custom_retry_limit)
            ),
        )
        create_connection_without_db(fake_conn)

        with SmtpHook(smtp_conn_id="mock_conn") as smtp_hook:
            with pytest.raises(smtplib.SMTPServerDisconnected):
                smtp_hook.send_email_smtp(to=TO_EMAIL, subject=TEST_SUBJECT, html_content=TEST_BODY)

        expected_call = call(
            host=fake_conn.host,
            port=fake_conn.port,
            timeout=fake_conn.extra_dejson["timeout"],
            context=create_default_context.return_value,
        )
        assert expected_call in mock_smtp_ssl.call_args_list
        assert create_default_context.called
        assert mock_smtp_ssl().sendmail.call_count == 10

    @patch(smtplib_string)
    def test_oauth2_auth_called(self, mock_smtplib):
        mock_conn = _create_fake_smtp(mock_smtplib, use_ssl=False)

        with SmtpHook(smtp_conn_id=CONN_ID_OAUTH, auth_type="oauth2") as smtp_hook:
            smtp_hook.send_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                from_email=FROM_EMAIL,
            )

        assert mock_conn.auth.called
        args, _ = mock_conn.auth.call_args
        assert args[0] == "XOAUTH2"
        assert build_xoauth2_string(SMTP_LOGIN, ACCESS_TOKEN) == args[1]()

    @patch(smtplib_string)
    def test_oauth2_missing_token_raises(self, mock_smtplib, create_connection_without_db):
        mock_conn = _create_fake_smtp(mock_smtplib, use_ssl=False)

        create_connection_without_db(
            Connection(
                conn_id="smtp_oauth2_empty",
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=NONSSL_PORT,
                extra=json.dumps(dict(disable_ssl=True, from_email=FROM_EMAIL)),
            )
        )

        with pytest.raises(AirflowException):
            with SmtpHook(smtp_conn_id="smtp_oauth2_empty", auth_type="oauth2") as h:
                h.send_email_smtp(
                    to=TO_EMAIL,
                    subject=TEST_SUBJECT,
                    html_content=TEST_BODY,
                    from_email=FROM_EMAIL,
                )

        assert not mock_conn.auth.called


@pytest.mark.asyncio
@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Async support was added to BaseNotifier in 3.1.0")
class TestSmtpHookAsync:
    """Tests for async functionality in SmtpHook."""

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_DEFAULT,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=DEFAULT_PORT,
                extra=json.dumps(dict(from_email=FROM_EMAIL, ssl_context="default")),
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ID_NONSSL,
                conn_type=CONN_TYPE,
                host=SMTP_HOST,
                login=SMTP_LOGIN,
                password=SMTP_PASSWORD,
                port=NONSSL_PORT,
                extra=json.dumps(dict(disable_ssl=True, from_email=FROM_EMAIL)),
            )
        )

    @pytest.fixture
    def mock_smtp_client(self):
        """Create a mock SMTP client with async capabilities."""
        mock_client = AsyncMock(spec=aiosmtplib.SMTP)
        mock_client.starttls = AsyncMock()
        mock_client.auth_login = AsyncMock()
        mock_client.sendmail = AsyncMock()
        mock_client.quit = AsyncMock()
        return mock_client

    @pytest.fixture
    def mock_smtp(self, mock_smtp_client):
        """Set up the SMTP mock with context manager."""
        with mock.patch("airflow.providers.smtp.hooks.smtp.aiosmtplib.SMTP") as mock_smtp:
            mock_smtp.return_value = mock_smtp_client
            yield mock_smtp

    @pytest.fixture
    def mock_get_connection(self):
        """Mock the async connection retrieval."""
        with mock.patch("airflow.sdk.bases.hook.BaseHook.aget_connection") as mock_conn:

            async def async_get_connection(conn_id):
                from airflow.sdk.definitions.connection import Connection

                return Connection.from_json(os.environ[f"AIRFLOW_CONN_{conn_id.upper()}"])

            mock_conn.side_effect = async_get_connection
            yield mock_conn

    @staticmethod
    def _create_fake_async_smtp(mock_smtp):
        mock_client = AsyncMock(spec=aiosmtplib.SMTP)
        mock_client.starttls = AsyncMock()
        mock_client.auth_login = AsyncMock()
        mock_client.sendmail = AsyncMock()
        mock_client.quit = AsyncMock()
        mock_smtp.return_value = mock_client
        return mock_client

    @pytest.mark.parametrize(
        ("conn_id", "expected_port", "expected_ssl"),
        [
            pytest.param(CONN_ID_NONSSL, NONSSL_PORT, False, id="non-ssl-connection"),
            pytest.param(CONN_ID_DEFAULT, DEFAULT_PORT, True, id="ssl-connection"),
        ],
    )
    async def test_async_connection(
        self, mock_smtp, mock_smtp_client, mock_get_connection, conn_id, expected_port, expected_ssl
    ):
        """Test async connection with different configurations."""
        async with SmtpHook(smtp_conn_id=conn_id) as hook:
            assert hook is not None

        mock_smtp.assert_called_once_with(
            hostname=SMTP_HOST,
            port=expected_port,
            timeout=DEFAULT_TIMEOUT,
            use_tls=expected_ssl,
            start_tls=None if expected_ssl else True,
        )

        if expected_ssl:
            assert mock_smtp_client.starttls.await_count == 1

        assert mock_smtp_client.auth_login.await_count == 1
        mock_smtp_client.auth_login.assert_awaited_once_with(SMTP_LOGIN, SMTP_PASSWORD)

    @pytest.mark.asyncio
    async def test_async_send_email(self, mock_smtp, mock_smtp_client, mock_get_connection):
        """Test async email sending functionality."""
        async with SmtpHook() as hook:
            await hook.asend_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
            )

        assert mock_smtp_client.sendmail.called
        #  The async version of sendmail only supports positional arguments
        #  for some reason, so we have to check these by positional args
        call_args = mock_smtp_client.sendmail.await_args.args
        assert call_args[0] == FROM_EMAIL  # sender is first positional arg
        assert call_args[1] == [TO_EMAIL]  # recipients is the second positional arg
        assert f"Subject: {TEST_SUBJECT}" in call_args[2]  # message is the third positional arg

    @pytest.mark.parametrize(
        ("side_effect", "expected_calls", "should_raise"),
        [
            pytest.param(
                [SERVER_DISCONNECTED_ERROR, SERVER_DISCONNECTED_ERROR, None],
                3,
                False,
                id="success_after_retries",
            ),
            pytest.param(SERVER_DISCONNECTED_ERROR, DEFAULT_RETRY_LIMIT, True, id="max_retries_exceeded"),
        ],
    )
    @pytest.mark.asyncio
    async def test_async_send_email_retries(
        self, mock_smtp, mock_smtp_client, mock_get_connection, side_effect, expected_calls, should_raise
    ):
        mock_smtp_client.sendmail.side_effect = side_effect

        if should_raise:
            with pytest.raises(aiosmtplib.errors.SMTPServerDisconnected):
                async with SmtpHook() as hook:
                    await hook.asend_email_smtp(
                        to=TO_EMAIL,
                        subject=TEST_SUBJECT,
                        html_content=TEST_BODY,
                    )
        else:
            async with SmtpHook() as hook:
                await hook.asend_email_smtp(
                    to=TO_EMAIL,
                    subject=TEST_SUBJECT,
                    html_content=TEST_BODY,
                )

        assert mock_smtp_client.sendmail.call_count == expected_calls

    async def test_async_send_email_dryrun(self, mock_smtp, mock_smtp_client, mock_get_connection):
        """Test async email sending in dryrun mode."""
        async with SmtpHook() as hook:
            await hook.asend_email_smtp(
                to=TO_EMAIL,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                dryrun=True,
            )

        mock_smtp_client.sendmail.assert_not_awaited()
