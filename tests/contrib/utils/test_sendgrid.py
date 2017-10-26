# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import unittest

from airflow.contrib.utils.sendgrid import send_email

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from mock import Mock
from mock import patch

class SendEmailSendGridTest(unittest.TestCase):
    # Unit test for sendgrid.send_email()
    def setUp(self):
        self.to = ['foo@foo.com', 'bar@bar.com']
        self.subject = 'sendgrid-send-email unit test'
        self.html_content = '<b>Foo</b> bar'
        self.cc = ['foo-cc@foo.com', 'bar-cc@bar.com']
        self.bcc = ['foo-bcc@foo.com', 'bar-bcc@bar.com']
        self.expected_mail_data = {
            'content': [{'type': u'text/html', 'value': '<b>Foo</b> bar'}],
            'personalizations': [
                {'cc': [{'email': 'foo-cc@foo.com'}, {'email': 'bar-cc@bar.com'}],
                 'to': [{'email': 'foo@foo.com'}, {'email': 'bar@bar.com'}],
                 'bcc': [{'email': 'foo-bcc@foo.com'}, {'email': 'bar-bcc@bar.com'}]}],
            'from': {'email': u'foo@bar.com'},
            'subject': 'sendgrid-send-email unit test'}

    # Test the right email is constructed.
    @mock.patch('os.environ.get')
    @mock.patch('airflow.contrib.utils.sendgrid._post_sendgrid_mail')
    def test_send_email_sendgrid_correct_email(self, mock_post, mock_get):
        mock_get.return_value = 'foo@bar.com'
        send_email(self.to, self.subject, self.html_content, cc=self.cc, bcc=self.bcc)
        mock_post.assert_called_with(self.expected_mail_data)
