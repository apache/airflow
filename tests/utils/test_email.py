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
import os
import tempfile
import unittest
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from smtplib import SMTPServerDisconnected
from unittest import mock

import pytest

from airflow import utils
from airflow.configuration import conf
from airflow.utils.email import build_mime_message, get_email_address_list
from tests.test_utils.config import conf_vars

EMAILS = ['test1@example.com', 'test2@example.com']

send_email_test = mock.MagicMock()


class TestEmail(unittest.TestCase):
    def test_get_email_address_single_email(self):
        emails_string = 'test1@example.com'

        assert get_email_address_list(emails_string) == [emails_string]

    def test_get_email_address_comma_sep_string(self):
        emails_string = 'test1@example.com, test2@example.com'

        assert get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_colon_sep_string(self):
        emails_string = 'test1@example.com; test2@example.com'

        assert get_email_address_list(emails_string) == EMAILS

    def test_get_email_address_list(self):
        emails_list = ['test1@example.com', 'test2@example.com']

        assert get_email_address_list(emails_list) == EMAILS

    def test_get_email_address_tuple(self):
        emails_tuple = ('test1@example.com', 'test2@example.com')

        assert get_email_address_list(emails_tuple) == EMAILS

    def test_get_email_address_invalid_type(self):
        emails_string = 1

        with pytest.raises(TypeError):
            get_email_address_list(emails_string)

    def test_get_email_address_invalid_type_in_iterable(self):
        emails_list = ['test1@example.com', 2]

        with pytest.raises(TypeError):
            get_email_address_list(emails_list)

    def setUp(self):
        conf.remove_option('email', 'EMAIL_BACKEND')

    @mock.patch('airflow.utils.email.send_email')
    def test_default_backend(self, mock_send_email):
        res = utils.email.send_email('to', 'subject', 'content')
        mock_send_email.assert_called_once_with('to', 'subject', 'content')
        assert mock_send_email.return_value == res

    @mock.patch('airflow.utils.email.send_email_smtp')
    def test_custom_backend(self, mock_send_email):
        with conf_vars({('email', 'email_backend'): 'tests.utils.test_email.send_email_test'}):
            utils.email.send_email('to', 'subject', 'content')
        send_email_test.assert_called_once_with(
            'to',
            'subject',
            'content',
            files=None,
            dryrun=False,
            cc=None,
            bcc=None,
            mime_charset='utf-8',
            mime_subtype='mixed',
        )
        assert not mock_send_email.called

    def test_build_mime_message(self):
        mail_from = 'from@example.com'
        mail_to = 'to@example.com'
        subject = 'test subject'
        html_content = '<html>Test</html>'
        custom_headers = {'Reply-To': 'reply_to@example.com'}

        msg, recipients = build_mime_message(
            mail_from=mail_from,
            to=mail_to,
            subject=subject,
            html_content=html_content,
            custom_headers=custom_headers,
        )

        assert 'From' in msg
        assert 'To' in msg
        assert 'Subject' in msg
        assert 'Reply-To' in msg
        assert [mail_to] == recipients
        assert msg['To'] == ','.join(recipients)


class TestEmailSmtp(unittest.TestCase):
    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name])
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == call_args['e_from']
        assert ['to'] == call_args['e_to']
        msg = call_args['mime_msg']
        assert 'subject' == msg['Subject']
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == msg['From']
        assert 2 == len(msg.get_payload())
        filename = 'attachment; filename="' + os.path.basename(attachment.name) + '"'
        assert filename == msg.get_payload()[-1].get('Content-Disposition')
        mimeapp = MIMEApplication('attachment')
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()

    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_smtp_with_multibyte_content(self, mock_send_mime):
        utils.email.send_email_smtp('to', 'subject', '🔥', mime_charset='utf-8')
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        msg = call_args['mime_msg']
        mimetext = MIMEText('🔥', 'mixed', 'utf-8')
        assert mimetext.get_payload() == msg.get_payload()[0].get_payload()

    @mock.patch('airflow.utils.email.send_mime_email')
    def test_send_bcc_smtp(self, mock_send_mime):
        attachment = tempfile.NamedTemporaryFile()
        attachment.write(b'attachment')
        attachment.seek(0)
        utils.email.send_email_smtp('to', 'subject', 'content', files=[attachment.name], cc='cc', bcc='bcc')
        assert mock_send_mime.called
        _, call_args = mock_send_mime.call_args
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == call_args['e_from']
        assert ['to', 'cc', 'bcc'] == call_args['e_to']
        msg = call_args['mime_msg']
        assert 'subject' == msg['Subject']
        assert conf.get('smtp', 'SMTP_MAIL_FROM') == msg['From']
        assert 2 == len(msg.get_payload())
        assert 'attachment; filename="' + os.path.basename(attachment.name) + '"' == msg.get_payload()[
            -1
        ].get('Content-Disposition')
        mimeapp = MIMEApplication('attachment')
        assert mimeapp.get_payload() == msg.get_payload()[-1].get_payload()

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        msg = MIMEMultipart()
        utils.email.send_mime_email('from', 'to', msg, dryrun=False)
        mock_smtp.assert_called_once_with(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )
        assert not mock_smtp_ssl.called
        assert mock_smtp.return_value.starttls.called
        mock_smtp.return_value.login.assert_called_once_with(
            conf.get('smtp', 'SMTP_USER'),
            conf.get('smtp', 'SMTP_PASSWORD'),
        )
        mock_smtp.return_value.sendmail.assert_called_once_with('from', 'to', msg.as_string())
        assert mock_smtp.return_value.quit.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl(self, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.return_value = mock.Mock()
        with conf_vars({('smtp', 'smtp_ssl'): 'True'}):
            utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=False)
        assert not mock_smtp.called
        mock_smtp_ssl.assert_called_once_with(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_noauth(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.return_value = mock.Mock()
        with conf_vars(
            {
                ('smtp', 'smtp_user'): None,
                ('smtp', 'smtp_password'): None,
            }
        ):
            utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=False)
        assert not mock_smtp_ssl.called
        mock_smtp.assert_called_once_with(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )
        assert not mock_smtp.login.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_dryrun(self, mock_smtp, mock_smtp_ssl):
        utils.email.send_mime_email('from', 'to', MIMEMultipart(), dryrun=True)
        assert not mock_smtp.called
        assert not mock_smtp_ssl.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_complete_failure(self, mock_smtp: mock, mock_smtp_ssl):
        mock_smtp.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()
        with pytest.raises(SMTPServerDisconnected):
            utils.email.send_mime_email('from', 'to', msg, dryrun=False)

        mock_smtp.assert_any_call(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )
        assert mock_smtp.call_count == conf.getint('smtp', 'SMTP_RETRY_LIMIT')
        assert not mock_smtp_ssl.called
        assert not mock_smtp.return_value.starttls.called
        assert not mock_smtp.return_value.login.called
        assert not mock_smtp.return_value.sendmail.called
        assert not mock_smtp.return_value.quit.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_ssl_complete_failure(self, mock_smtp, mock_smtp_ssl):
        mock_smtp_ssl.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()
        with conf_vars({('smtp', 'smtp_ssl'): 'True'}):
            with pytest.raises(SMTPServerDisconnected):
                utils.email.send_mime_email('from', 'to', msg, dryrun=False)

        mock_smtp_ssl.assert_any_call(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )
        assert mock_smtp_ssl.call_count == conf.getint('smtp', 'SMTP_RETRY_LIMIT')
        assert not mock_smtp.called
        assert not mock_smtp_ssl.return_value.starttls.called
        assert not mock_smtp_ssl.return_value.login.called
        assert not mock_smtp_ssl.return_value.sendmail.called
        assert not mock_smtp_ssl.return_value.quit.called

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_custom_timeout_retrylimit(self, mock_smtp, mock_smtp_ssl):
        mock_smtp.side_effect = SMTPServerDisconnected()
        msg = MIMEMultipart()

        custom_retry_limit = 10
        custom_timeout = 60

        with conf_vars(
            {
                ('smtp', 'smtp_retry_limit'): str(custom_retry_limit),
                ('smtp', 'smtp_timeout'): str(custom_timeout),
            }
        ):
            with pytest.raises(SMTPServerDisconnected):
                utils.email.send_mime_email('from', 'to', msg, dryrun=False)

        mock_smtp.assert_any_call(
            host=conf.get('smtp', 'SMTP_HOST'), port=conf.getint('smtp', 'SMTP_PORT'), timeout=custom_timeout
        )
        assert not mock_smtp_ssl.called
        assert mock_smtp.call_count == 10

    @mock.patch('smtplib.SMTP_SSL')
    @mock.patch('smtplib.SMTP')
    def test_send_mime_partial_failure(self, mock_smtp, mock_smtp_ssl):
        final_mock = mock.Mock()
        side_effects = [SMTPServerDisconnected(), SMTPServerDisconnected(), final_mock]
        mock_smtp.side_effect = side_effects
        msg = MIMEMultipart()

        utils.email.send_mime_email('from', 'to', msg, dryrun=False)

        mock_smtp.assert_any_call(
            host=conf.get('smtp', 'SMTP_HOST'),
            port=conf.getint('smtp', 'SMTP_PORT'),
            timeout=conf.getint('smtp', 'SMTP_TIMEOUT'),
        )
        assert mock_smtp.call_count == side_effects.index(final_mock) + 1
        assert not mock_smtp_ssl.called
        assert final_mock.starttls.called
        final_mock.sendmail.assert_called_once_with('from', 'to', msg.as_string())
        assert final_mock.quit.called
