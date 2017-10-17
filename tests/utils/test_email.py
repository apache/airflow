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

from airflow.utils.email import send_email_sendgrid

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
    # Unit test for send_email_sendgrid()
    def setUp(self):
        self.to = ['foo@foo.com', 'bar@bar.com']
        self.subject = 'send-email-sendgrid unit test'
        self.html_content = '<b>Foo</b> bar'
        self.expected_mail_data = {
            'content': [{'type': u'text/html', 'value': '<b>Foo</b> bar'}],
            'personalizations': [
                {'to': [{'email': 'foo@foo.com'}, {'email': 'bar@bar.com'}]}],
            'from': {'email': u'foo@bar.com'},
            'subject': 'send-email-sendgrid unit test'}

    # Test the right email is constructed.
    @mock.patch('airflow.configuration.get')
    @mock.patch('airflow.utils.email._post_sendgrid_mail')
    def test_send_email_sendgrid_correct_email(self, mock_post, mock_get):
        mock_get.return_value = 'foo@bar.com'
        send_email_sendgrid(self.to, self.subject, self.html_content)
        mock_post.assert_called_with(self.expected_mail_data)
