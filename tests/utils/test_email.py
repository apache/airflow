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
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPServerDisconnected
from unittest import mock

import pytest

from airflow.configuration import conf
from airflow.utils import email
from tests_common.test_utils.config import conf_vars

EMAILS = ["test1@example.com", "test2@example.com"]

send_email_test = mock.MagicMock()

pytestmark = pytest.mark.skip_if_database_isolation_mode


class TestEmail:
    def setup_method(self):
        conf.remove_option("email", "EMAIL_BACKEND")
        conf.remove_option("email", "EMAIL_CONN_ID")

    def test_get_email_address_single_email(self):
        emails_string = "test1@example.com"

        assert email.get_email_address_list(emails_string) == [emails_string]

    def test_get_email_address_comma_sep_string(self):
        emails_string = "test1@example.com, test2@example.com"

        assert email.get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_colon_sep_string(self):
        emails_string = "test1@example.com; test2@example.com"

        assert email.get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_list(self):
        emails_list = ["test1@example.com", "test2@example.com"]

        assert email.get_email_address_list(emails_list) == EMAILS

    def test_get_email_address_tuple(self):
        emails_tuple = ("test1@example.com", "test2@example.com")

        assert email.get_email_address_list(emails_tuple) == EMAILS

    def test_get_email_address_invalid_type(self):
        emails_string = 1

        with pytest.raises(TypeError):
            email.get_email_address_list(emails_string)

    def test_get_email_address_invalid_type_in_iterable(self):
        emails_list = ["test1@example.com", 2]

        with pytest.raises(TypeError):
            email.get_email_address_list(emails_list)

    @mock.patch("airflow.utils.email.send_email")
    def test_default_backend(self, mock_send_email):
        res = email.send_email("to", "subject", "content")
        mock_send_email.assert_called_once_with("to", "subject", "content")
        assert mock_send_email.return_value == res

    @mock.patch("airflow.utils.email.send_email_smtp")
    def test_custom_backend(self, mock_send_email):
        with conf_vars(
            {
                ("email", "email_backend"): "tests.utils.test_email.send_email_test",
                ("email", "email_conn_id"): "smtp_default",
            }
        ):
            email.send_email("to", "subject", "content")
        send_email_test.assert_called_once_with(
            "to",
            "subject",
            "content",
            files=None,
            dryrun=False,
            cc=None,
            bcc=None,
            mime_charset="utf-8",
            mime_subtype="mixed",
            conn_id="smtp_default",
            from_email=None,
            custom_headers=None,
        )
        assert not mock_send_email.called

    @mock.patch("airflow.utils.email.send_email_smtp")
    @conf_vars(
        {
            ("email", "email_backend"): "tests.utils.test_email.send_email_test",
            ("email", "email_conn_id"): "smtp_default",
            ("email", "from_email"): "from@test.com",
        }
    )
    def test_custom_backend_sender(self, mock_send_email_smtp):
        email.send_email("to", "subject", "content")
        _, call_kwargs = send_email_test.call_args
        assert call_kwargs["from_email"] == "from@test.com"
        assert not mock_send_email_smtp.called

    def test_build_mime_message(self):
        mail_from = "from@example.com"
        mail_to = "to@example.com"
        subject = "test subject"
        html_content = "<html>Test</html>"
        custom_headers = {"Reply-To": "reply_to@example.com"}

        msg, recipients = email.build_mime_message(
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


@pytest.mark.db_test
class TestEmailSmtp:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        monkeypatch.setenv(  # Set the default smtp connection for all test cases
            "AIRFLOW_CONN_SMTP_DEFAULT",
            json.dumps({"conn_type": "smtp", "login": "user", "password": "p@$$word"}),
        )

    @mock.patch("airflow.utils.email.send_mime_email")
    def test_send_smtp(self, mock_send_mime, tmp_path):
        path = tmp_path / "testfile"
        path.write_text("attachment")
        email.send_email_smtp("to", "subject", "content", files=[os.fspath(path)])
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        assert conf.get("smtp", "SMTP_MAIL_FROM") == call_args["e_from"]
        assert ["to"] == call_args["e_to"]
        msg = call_args["mime_msg"]
        assert "subject" == msg["Subject"]
        assert conf.get("smtp", "SMTP_MAIL_FROM") == msg["From"]
        assert 2 == len(msg.get_payload())
        filename = f'attachment; filename="{path.name}"'
        assert filename == msg.get_payload()[-1].get("Content-Disposition")
        mimeapp = MIMEApplication("attachment")
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()

    @mock.patch("airflow.utils.email.send_mime_email")
    def test_send_smtp_with_multibyte_content(self, mock_send_mime):
        email.send_email_smtp("to", "subject", "🔥", mime_charset="utf-8")
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        msg = call_args["mime_msg"]
        mimetext = MIMEText("🔥", "mixed", "utf-8")
        assert mimetext.get_payload() == msg.get_payload()[0].get_payload()

    @mock.patch("airflow.utils.email.send_mime_email")
    def test_send_bcc_smtp(self, mock_send_mime, tmp_path):
        path = tmp_path / "testfile"
        path.write_text("attachment")
        email.send_email_smtp(
            "to",
            "subject",
            "content",
            files=[os.fspath(path)],
            cc="cc",
            bcc="bcc",
            custom_headers={"Reply-To": "reply_to@example.com"},
        )
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        assert conf.get("smtp", "SMTP_MAIL_FROM") == call_args["e_from"]
        assert ["to", "cc", "bcc"] == call_args["e_to"]
        msg = call_args["mime_msg"]
        assert "subject" == msg["Subject"]
        assert conf.get("smtp", "SMTP_MAIL_FROM") == msg["From"]
        assert 2 == len(msg.get_payload())
        assert f'attachment; filename="{path.name}"' == msg.get_payload()[-1].get("Content-Disposition")
        mimeapp = MIMEApplication("attachment")
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()
        assert msg["Reply-To"] == "reply_to@example.com"

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_airflow_config(self, mock_smtp, mock_smtp_ssl, monkeypatch):
        monkeypatch.delenv("AIRFLOW_CONN_SMTP_DEFAULT", raising=False)
        mock_smtp.return_value = mock.Mock()
        msg = MIMEMultipart()
        email.send_mime_email("from", "to", msg, dryrun=False)
        mock_smtp.assert_called_once_with(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
        )
        assert not mock_smtp_ssl.called
        assert mock_smtp.return_value.starttls.called
        mock_smtp.return_value.sendmail.assert_called_once_with("from", "to", msg.as_string())
        assert mock_smtp.return_value.quit.called

    @mock.patch("smtplib.SMTP")
    def test_send_mime_conn_id(self, mock_smtp, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_SMTP_TEST_CONN",
            json.dumps({"conn_type": "smtp", "login": "test-user", "password": "test-p@$$word"}),
        )
        msg = MIMEMultipart()
        email.send_mime_email("from", "to", msg, dryrun=False, conn_id="smtp_test_conn")
        mock_smtp.return_value.login.assert_called_once_with("test-user", "test-p@$$word")
        mock_smtp.return_value.sendmail.assert_called_once_with("from", "to", msg.as_string())
        assert mock_smtp.return_value.quit.called

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_ssl_none_context(self, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({("smtp", "smtp_ssl"): "True", ("email", "ssl_context"): "none"}):
            email.send_mime_email("from", "to", MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        mock_smtp_ssl.assert_called_once_with(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
            context=None,
        )

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    @mock.patch("ssl.create_default_context")
    def test_send_mime_ssl_default_context_if_not_set(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({("smtp", "smtp_ssl"): "True"}):
            email.send_mime_email("from", "to", MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        assert create_default_context.called
        mock_smtp_ssl.assert_called_once_with(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
            context=create_default_context.return_value,
        )

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    @mock.patch("ssl.create_default_context")
    def test_send_mime_ssl_default_context_with_value_set_to_default(
        self, create_default_context, mock_smtp, mock_smtp_ssl
    ):
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({("smtp", "smtp_ssl"): "True", ("email", "ssl_context"): "default"}):
            email.send_mime_email("from", "to", MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        assert create_default_context.called
        mock_smtp_ssl.assert_called_once_with(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
            context=create_default_context.return_value,
        )

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_noauth(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        with conf_vars(
            {
                ("smtp", "smtp_user"): None,
                ("smtp", "smtp_password"): None,
            }
        ):
            email.send_mime_email("from", "to", MIMEMultipart(), dryrun=False)
        assert not mock_smtp_ssl.called
        mock_smtp.assert_called_once_with(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
        )
        assert not mock_smtp.login.called

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        email.send_mime_email("from", "to", MIMEMultipart(), dryrun=True)
        assert not mock_smtp.called
        assert not mock_smtp_ssl.called

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_complete_failure(self, mock_smtp: mock.Mock, mock_smtp_ssl: mock.Mock):
        mock_smtp.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()
        with pytest.raises(SMTPServerDisconnected):
            email.send_mime_email("from", "to", msg, dryrun=False)
        mock_smtp.assert_any_call(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
        )
        assert mock_smtp.call_count == conf.getint("smtp", "SMTP_RETRY_LIMIT")
        assert not mock_smtp_ssl.called
        assert not mock_smtp.return_value.starttls.called
        assert not mock_smtp.return_value.login.called
        assert not mock_smtp.return_value.sendmail.called
        assert not mock_smtp.return_value.quit.called

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    @mock.patch("ssl.create_default_context")
    def test_send_mime_ssl_complete_failure(self, create_default_context, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()
        with conf_vars({("smtp", "smtp_ssl"): "True"}):
            with pytest.raises(SMTPServerDisconnected):
                email.send_mime_email("from", "to", msg, dryrun=False)

        mock_smtp_ssl.assert_any_call(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
            context=create_default_context.return_value,
        )
        assert create_default_context.called
        assert mock_smtp_ssl.call_count == conf.getint("smtp", "SMTP_RETRY_LIMIT")
        assert not mock_smtp.called
        assert not mock_smtp_ssl.return_value.starttls.called
        assert not mock_smtp_ssl.return_value.login.called
        assert not mock_smtp_ssl.return_value.sendmail.called
        assert not mock_smtp_ssl.return_value.quit.called

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_custom_timeout_retrylimit(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()

        custom_retry_limit = 10
        custom_timeout = 60

        with conf_vars(
            {
                ("smtp", "smtp_retry_limit"): str(custom_retry_limit),
                ("smtp", "smtp_timeout"): str(custom_timeout),
            }
        ):
            with pytest.raises(SMTPServerDisconnected):
                email.send_mime_email("from", "to", msg, dryrun=False)

        mock_smtp.assert_any_call(
            host=conf.get("smtp", "SMTP_HOST"), port=conf.getint("smtp", "SMTP_PORT"), timeout=custom_timeout
        )
        assert not mock_smtp_ssl.called
        assert mock_smtp.call_count == 10

    @mock.patch("smtplib.SMTP_SSL")
    @mock.patch("smtplib.SMTP")
    def test_send_mime_partial_failure(self, mock_smtp, mock_smtp_ssl):
        final_mock = mock.Mock()
        side_effects = [SMTPServerDisconnected(), SMTPServerDisconnected(), final_mock]
        mock_smtp.side_effect = side_effects
        msg = MIMEMultipart()

        email.send_mime_email("from", "to", msg, dryrun=False)

        mock_smtp.assert_any_call(
            host=conf.get("smtp", "SMTP_HOST"),
            port=conf.getint("smtp", "SMTP_PORT"),
            timeout=conf.getint("smtp", "SMTP_TIMEOUT"),
        )
        assert mock_smtp.call_count == side_effects.index(final_mock) + 1
        assert not mock_smtp_ssl.called
        assert final_mock.starttls.called
        final_mock.sendmail.assert_called_once_with("from", "to", msg.as_string())
        assert final_mock.quit.called
