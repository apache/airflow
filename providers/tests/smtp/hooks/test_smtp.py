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
from unittest.mock import Mock, patch

import pytest

from airflow.models import Connection
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.utils import db
from airflow.utils.session import create_session

from dev.tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


smtplib_string = "airflow.providers.smtp.hooks.smtp.smtplib"

TEST_EMAILS = ["test1@example.com", "test2@example.com"]


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
    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id="smtp_default",
                conn_type="smtp",
                host="smtp_server_address",
                login="smtp_user",
                password="smtp_password",
                port=465,
                extra=json.dumps(dict(from_email="from")),
            )
        )
        db.merge_conn(
            Connection(
                conn_id="smtp_nonssl",
                conn_type="smtp",
                host="smtp_server_address",
                login="smtp_user",
                password="smtp_password",
                port=587,
                extra=json.dumps(dict(disable_ssl=True, from_email="from")),
            )
        )

    @patch(smtplib_string)
    @patch("ssl.create_default_context")
    def test_connect_and_disconnect(self, create_default_context, mock_smtplib):
        mock_conn = _create_fake_smtp(mock_smtplib)

        with SmtpHook():
            pass
        assert create_default_context.called
        mock_smtplib.SMTP_SSL.assert_called_once_with(
            host="smtp_server_address", port=465, timeout=30, context=create_default_context.return_value
        )
        mock_conn.login.assert_called_once_with("smtp_user", "smtp_password")
        assert mock_conn.close.call_count == 1

    @patch(smtplib_string)
    def test_connect_and_disconnect_via_nonssl(self, mock_smtplib):
        mock_conn = _create_fake_smtp(mock_smtplib, use_ssl=False)

        with SmtpHook(smtp_conn_id="smtp_nonssl"):
            pass

        mock_smtplib.SMTP.assert_called_once_with(host="smtp_server_address", port=587, timeout=30)
        mock_conn.login.assert_called_once_with("smtp_user", "smtp_password")
        assert mock_conn.close.call_count == 1

    @patch(smtplib_string)
    def test_get_email_address_single_email(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com") == ["test1@example.com"]

    @patch(smtplib_string)
    def test_get_email_address_comma_sep_string(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com, test2@example.com") == TEST_EMAILS

    @patch(smtplib_string)
    def test_get_email_address_colon_sep_string(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert smtp_hook._get_email_address_list("test1@example.com; test2@example.com") == TEST_EMAILS

    @patch(smtplib_string)
    def test_get_email_address_list(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert (
                smtp_hook._get_email_address_list(["test1@example.com", "test2@example.com"]) == TEST_EMAILS
            )

    @patch(smtplib_string)
    def test_get_email_address_tuple(self, mock_smtplib):
        with SmtpHook() as smtp_hook:
            assert (
                smtp_hook._get_email_address_list(("test1@example.com", "test2@example.com")) == TEST_EMAILS
            )

    @patch(smtplib_string)
    def test_get_email_address_invalid_type(self, mock_smtplib):
        with pytest.raises(TypeError):
            with SmtpHook() as smtp_hook:
                smtp_hook._get_email_address_list(1)

    @patch(smtplib_string)
    def test_get_email_address_invalid_type_in_iterable(self, mock_smtplib):
        with pytest.raises(TypeError):
            with SmtpHook() as smtp_hook:
                smtp_hook._get_email_address_list(["test1@example.com", 2])

    @patch(smtplib_string)
    def test_build_mime_message(self, mock_smtplib):
        mail_from = "from@example.com"
        mail_to = "to@example.com"
        subject = "test subject"
        html_content = "<html>Test</html>"
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
                to="to", subject="subject", html_content="content", files=[attachment.name]
            )
            assert mock_send_mime.called
            _, call_args = mock_send_mime.call_args
            assert "from" == call_args["from_addr"]
            assert ["to"] == call_args["to_addrs"]
            msg = call_args["msg"]
            assert "Subject: subject" in msg
            assert "From: from" in msg
            filename = 'attachment; filename="' + os.path.basename(attachment.name) + '"'
            assert filename in msg
            mimeapp = MIMEApplication("attachment")
            assert mimeapp.get_payload() in msg

    @patch("airflow.providers.smtp.hooks.smtp.SmtpHook.get_connection")
    @patch(smtplib_string)
    def test_hook_conn(self, mock_smtplib, mock_hook_conn):
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "password"
        mock_conn.extra_dejson = {
            "disable_ssl": False,
        }
        mock_hook_conn.return_value = mock_conn
        smtp_client_mock = mock_smtplib.SMTP_SSL()
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content", from_email="from")
            mock_hook_conn.assert_called_with("smtp_default")
            smtp_client_mock.login.assert_called_once_with("user", "password")
            smtp_client_mock.sendmail.assert_called_once()
        assert smtp_client_mock.close.called

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = Mock()
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content", from_email="from")
        assert not mock_smtp.called
        assert create_default_context.called
        mock_smtp_ssl.assert_called_once_with(
            host="smtp_server_address", port=465, timeout=30, context=create_default_context.return_value
        )

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_none_email_context(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = Mock()
        with conf_vars({("smtp", "smtp_ssl"): "True", ("email", "ssl_context"): "none"}):
            with SmtpHook() as smtp_hook:
                smtp_hook.send_email_smtp(
                    to="to", subject="subject", html_content="content", from_email="from"
                )
        assert not mock_smtp.called
        assert not create_default_context.called
        mock_smtp_ssl.assert_called_once_with(host="smtp_server_address", port=465, timeout=30, context=None)

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_extra_context(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = Mock()
        conn = Connection(
            conn_id="smtp_ssl_extra",
            conn_type="smtp",
            host="smtp_server_address",
            login=None,
            password="None",
            port=465,
            extra=json.dumps(dict(ssl_context="none", from_email="from")),
        )
        db.merge_conn(conn)
        with conf_vars({("smtp", "smtp_ssl"): "True", ("smtp_provider", "ssl_context"): "default"}):
            with SmtpHook(smtp_conn_id="smtp_ssl_extra") as smtp_hook:
                smtp_hook.send_email_smtp(
                    to="to", subject="subject", html_content="content", from_email="from"
                )
        assert not mock_smtp.called
        assert not create_default_context.called
        mock_smtp_ssl.assert_called_once_with(host="smtp_server_address", port=465, timeout=30, context=None)

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_none_smtp_provider_context(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = Mock()
        with conf_vars({("smtp", "smtp_ssl"): "True", ("smtp_provider", "ssl_context"): "none"}):
            with SmtpHook() as smtp_hook:
                smtp_hook.send_email_smtp(
                    to="to", subject="subject", html_content="content", from_email="from"
                )
        assert not mock_smtp.called
        assert not create_default_context.called
        mock_smtp_ssl.assert_called_once_with(host="smtp_server_address", port=465, timeout=30, context=None)

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_none_smtp_provider_default_email_context(
        self, create_default_context, mock_smtp, mock_smtp_ssl
    ):
        mock_smtp_ssl.return_value = Mock()
        with conf_vars(
            {
                ("smtp", "smtp_ssl"): "True",
                ("email", "ssl_context"): "default",
                ("smtp_provider", "ssl_context"): "none",
            }
        ):
            with SmtpHook() as smtp_hook:
                smtp_hook.send_email_smtp(
                    to="to", subject="subject", html_content="content", from_email="from"
                )
        assert not mock_smtp.called
        assert not create_default_context.called
        mock_smtp_ssl.assert_called_once_with(host="smtp_server_address", port=465, timeout=30, context=None)

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    @patch("ssl.create_default_context")
    def test_send_mime_ssl_default_smtp_provider_none_email_context(
        self, create_default_context, mock_smtp, mock_smtp_ssl
    ):
        mock_smtp_ssl.return_value = Mock()
        with conf_vars(
            {
                ("smtp", "smtp_ssl"): "True",
                ("email", "ssl_context"): "none",
                ("smtp_provider", "ssl_context"): "default",
            }
        ):
            with SmtpHook() as smtp_hook:
                smtp_hook.send_email_smtp(
                    to="to", subject="subject", html_content="content", from_email="from"
                )
        assert not mock_smtp.called
        assert create_default_context.called
        mock_smtp_ssl.assert_called_once_with(
            host="smtp_server_address", port=465, timeout=30, context=create_default_context.return_value
        )

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    def test_send_mime_nossl(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = Mock()
        with SmtpHook(smtp_conn_id="smtp_nonssl") as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content", from_email="from")
        assert not mock_smtp_ssl.called
        mock_smtp.assert_called_once_with(host="smtp_server_address", port=587, timeout=30)

    @patch("smtplib.SMTP")
    def test_send_mime_noauth(self, mock_smtp):
        mock_smtp.return_value = Mock()
        conn = Connection(
            conn_id="smtp_noauth",
            conn_type="smtp",
            host="smtp_server_address",
            login=None,
            password="None",
            port=587,
            extra=json.dumps(dict(disable_ssl=True, from_email="from")),
        )
        db.merge_conn(conn)
        with SmtpHook(smtp_conn_id="smtp_noauth") as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content", from_email="from")
        mock_smtp.assert_called_once_with(host="smtp_server_address", port=587, timeout=30)
        assert not mock_smtp.login.called
        with create_session() as session:
            session.query(Connection).filter(Connection.id == conn.id).delete()

    @patch("smtplib.SMTP_SSL")
    @patch("smtplib.SMTP")
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content", dryrun=True)
        assert not mock_smtp.sendmail.called
        assert not mock_smtp_ssl.sendmail.called

    @patch("smtplib.SMTP_SSL")
    def test_send_mime_ssl_complete_failure(self, mock_smtp_ssl):
        mock_smtp_ssl().sendmail.side_effect = smtplib.SMTPServerDisconnected()
        with SmtpHook() as smtp_hook:
            with pytest.raises(smtplib.SMTPServerDisconnected):
                smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content")
        assert mock_smtp_ssl().sendmail.call_count == 5

    @patch("email.message.Message.as_string")
    @patch("smtplib.SMTP_SSL")
    def test_send_mime_partial_failure(self, mock_smtp_ssl, mime_message_mock):
        mime_message_mock.return_value = "msg"
        final_mock = Mock()
        side_effects = [smtplib.SMTPServerDisconnected(), smtplib.SMTPServerDisconnected(), final_mock]
        mock_smtp_ssl.side_effect = side_effects
        with SmtpHook() as smtp_hook:
            smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content")
        assert mock_smtp_ssl.call_count == side_effects.index(final_mock) + 1
        assert final_mock.starttls.called
        final_mock.sendmail.assert_called_once_with(from_addr="from", to_addrs=["to"], msg="msg")
        assert final_mock.close.called

    @patch("airflow.models.connection.Connection")
    @patch("smtplib.SMTP_SSL")
    @patch("ssl.create_default_context")
    def test_send_mime_custom_timeout_retrylimit(
        self, create_default_context, mock_smtp_ssl, connection_mock
    ):
        mock_smtp_ssl().sendmail.side_effect = smtplib.SMTPServerDisconnected()
        custom_retry_limit = 10
        custom_timeout = 60
        fake_conn = Connection(
            conn_id="mock_conn",
            conn_type="smtp",
            host="smtp_server_address",
            login="smtp_user",
            password="smtp_password",
            port=465,
            extra=json.dumps(dict(from_email="from", timeout=custom_timeout, retry_limit=custom_retry_limit)),
        )
        connection_mock.get_connection_from_secrets.return_value = fake_conn
        with SmtpHook() as smtp_hook:
            with pytest.raises(smtplib.SMTPServerDisconnected):
                smtp_hook.send_email_smtp(to="to", subject="subject", html_content="content")
        mock_smtp_ssl.assert_any_call(
            host=fake_conn.host,
            port=fake_conn.port,
            timeout=fake_conn.extra_dejson["timeout"],
            context=create_default_context.return_value,
        )
        assert create_default_context.called
        assert mock_smtp_ssl().sendmail.call_count == 10
