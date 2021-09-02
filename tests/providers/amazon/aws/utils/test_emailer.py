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
from unittest import mock, TestCase

from airflow.providers.amazon.aws.utils.emailer import send_email
from tests.test_utils.config import conf_vars


class TestSendEmailSes(TestCase):
    def setUp(self):
        pass

    @mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
    def test_send_ses_email(self, mock_hook):
        send_email(
            to="to@test.com",
            subject="subject",
            html_content="content"
        )

        mock_hook.return_value.send_email.assert_called_once_with(
            mail_from=None,
            to="to@test.com",
            subject="subject",
            html_content="content",
            bcc=None,
            cc=None,
            files=None,
            mime_charset="utf-8",
            mime_subtype="mixed",
        )

    @mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
    def test_send_ses_email_sender_kwargs(self, mock_hook):
        send_email(
            to="to@test.com",
            subject="subject",
            html_content="content",
            from_email='from.kwargs@test.com',
            from_name='From Kwargs'
        )

        _, call_args = mock_hook.return_value.send_email.call_args

        assert call_args['mail_from'] == "From Kwargs <from.kwargs@test.com>"

    @mock.patch.dict('os.environ', SES_MAIL_FROM='from.env@test.com', SES_MAIL_SENDER='From Env')
    @mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
    def test_send_ses_email_sender_env(self, mock_hook):
        send_email(
            to="to@test.com",
            subject="subject",
            html_content="content",
        )

        _, call_args = mock_hook.return_value.send_email.call_args
        assert call_args['mail_from'] == "From Env <from.env@test.com>"

    @conf_vars({('ses', 'ses_mail_from'): 'from.conf@test.com',
                ('ses', 'ses_mail_sender'): 'From Conf'})
    @mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
    def test_send_ses_email_sender_conf(self, mock_hook):
        send_email(
            to="to@test.com",
            subject="subject",
            html_content="content",
        )

        _, call_args = mock_hook.return_value.send_email.call_args
        assert call_args['mail_from'] == "From Conf <from.conf@test.com>"

    @conf_vars({('ses', 'ses_mail_from'): 'from.conf@test.com'})
    @mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
    def test_send_ses_email_sender_conf_without_name(self, mock_hook):
        send_email(
            to="to@test.com",
            subject="subject",
            html_content="content",
        )

        _, call_args = mock_hook.return_value.send_email.call_args
        assert call_args['mail_from'] == "from.conf@test.com"
