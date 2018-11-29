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

import os
import unittest

from airflow import configuration
from airflow.contrib.utils.ses import send_email
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class SendEmailSesTest(unittest.TestCase):
    # Unit test for ses.send_email()
    def setUp(self):
        configuration.set('smtp', 'SMTP_MAIL_FROM', 'foo@bar.com')

        self.to = ['foo@foo.com', 'bar@bar.com']
        self.subject = 'ses-send-email unit test'
        self.html_content = '<b>Foo</b> bar'
        self.cc = ['foo-cc@foo.com', 'bar-cc@bar.com']
        self.bcc = ['foo-bcc@foo.com', 'bar-bcc@bar.com']

        self.expected_message = MIMEMultipart('mixed')
        self.expected_message['Subject'] = 'ses-send-email unit test'
        self.expected_message['From'] = 'foo@bar.com'
        self.expected_message['To'] = 'foo@foo.com, bar@bar.com'
        self.expected_message['Cc'] = 'foo-cc@foo.com, bar-cc@bar.com'
        self.expected_message['Bcc'] = 'foo-bcc@foo.com, bar-bcc@bar.com'
        self.expected_message['Date'] = formatdate(localtime=True)
        self.expected_message.attach(MIMEText(self.html_content, 'html'))

    @mock.patch('airflow.contrib.utils.ses._ses_send_raw_email')
    def test_send_email_ses_correct_message(self, mock_send):
        send_email(self.to,
                   self.subject,
                   self.html_content,
                   cc=self.cc,
                   bcc=self.bcc)
        # TODO fix test: failing because the MIME boundary is randomly generated
        mock_send.assert_called_with(self.expected_message)
