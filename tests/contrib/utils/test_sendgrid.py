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

import copy
import unittest
import tempfile
import os

from airflow.contrib.utils.sendgrid import send_email
from tests.compat import mock


class TestSendEmailSendGrid(unittest.TestCase):
    # Unit test for sendgrid.send_email()
    def setUp(self):
        self.to = ['foo@foo.com', 'bar@bar.com']
        self.subject = 'sendgrid-send-email unit test'
        self.html_content = '<b>Foo</b> bar'
        self.cc = ['foo-cc@foo.com', 'bar-cc@bar.com']
        self.bcc = ['foo-bcc@foo.com', 'bar-bcc@bar.com']
        self.expected_mail_data = {
            'content': [{'type': 'text/html', 'value': self.html_content}],
            'personalizations': [
                {'cc': [{'email': 'foo-cc@foo.com'}, {'email': 'bar-cc@bar.com'}],
                 'to': [{'email': 'foo@foo.com'}, {'email': 'bar@bar.com'}],
                 'bcc': [{'email': 'foo-bcc@foo.com'}, {'email': 'bar-bcc@bar.com'}]}],
            'from': {'email': 'foo@bar.com'},
            'subject': 'sendgrid-send-email unit test',
            'mail_settings': {},
        }
        self.personalization_custom_args = {'arg1': 'val1', 'arg2': 'val2'}
        self.categories = ['cat1', 'cat2']
        # extras
        self.expected_mail_data_extras = copy.deepcopy(self.expected_mail_data)
        self.expected_mail_data_extras['personalizations'][0]['custom_args'] = (
            self.personalization_custom_args)
        self.expected_mail_data_extras['categories'] = self.categories
        self.expected_mail_data_extras['from'] = {
            'name': 'Foo',
            'email': 'foo@bar.com',
        }
        # sender
        self.expected_mail_data_sender = copy.deepcopy(self.expected_mail_data)
        self.expected_mail_data_sender['from'] = {
            'name': 'Foo Bar',
            'email': 'foo@foo.bar',
        }

    # Test the right email is constructed.
    @mock.patch('os.environ', dict(os.environ, SENDGRID_MAIL_FROM='foo@bar.com'))
    @mock.patch('airflow.contrib.utils.sendgrid._post_sendgrid_mail')
    def test_send_email_sendgrid_correct_email(self, mock_post):
        with tempfile.NamedTemporaryFile(mode='wt', suffix='.txt') as f:
            f.write('this is some test data')
            f.flush()

            filename = os.path.basename(f.name)
            expected_mail_data = dict(
                self.expected_mail_data,
                attachments=[{
                    'content': 'dGhpcyBpcyBzb21lIHRlc3QgZGF0YQ==',
                    'content_id': '<{0}>'.format(filename),
                    'disposition': 'attachment',
                    'filename': filename,
                    'type': 'text/plain',
                }],
            )

            send_email(self.to,
                       self.subject,
                       self.html_content,
                       cc=self.cc,
                       bcc=self.bcc,
                       files=[f.name])
            mock_post.assert_called_once_with(expected_mail_data)

    # Test the right email is constructed.
    @mock.patch(
        'os.environ',
        dict(os.environ,
             SENDGRID_MAIL_FROM='foo@bar.com',
             SENDGRID_MAIL_SENDER='Foo')
    )
    @mock.patch('airflow.contrib.utils.sendgrid._post_sendgrid_mail')
    def test_send_email_sendgrid_correct_email_extras(self, mock_post):
        send_email(self.to, self.subject, self.html_content, cc=self.cc, bcc=self.bcc,
                   personalization_custom_args=self.personalization_custom_args,
                   categories=self.categories)
        mock_post.assert_called_once_with(self.expected_mail_data_extras)

    @mock.patch('os.environ', {})
    @mock.patch('airflow.contrib.utils.sendgrid._post_sendgrid_mail')
    def test_send_email_sendgrid_sender(self, mock_post):
        send_email(self.to, self.subject, self.html_content, cc=self.cc, bcc=self.bcc,
                   from_email='foo@foo.bar', from_name='Foo Bar')
        mock_post.assert_called_once_with(self.expected_mail_data_sender)
