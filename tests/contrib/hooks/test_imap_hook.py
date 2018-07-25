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

import imaplib
import unittest
from unittest.mock import Mock, patch

from airflow import configuration, models
from airflow.contrib.hooks.imap_hook import ImapHook
from airflow.utils import db


class TestImapHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id='imap_default',
                host='imap_server_address',
                login='imap_user',
                password='imap_password'
            )
        )

    @patch('airflow.contrib.hooks.imap_hook.imaplib')
    def test_connect(self, mock_imaplib):
        mock_conn = Mock(spec=imaplib.IMAP4_SSL)
        mock_imaplib.IMAP4_SSL.return_value = mock_conn
        mock_conn.login.return_value = ('OK', [])

        with ImapHook():
            pass

        mock_imaplib.IMAP4_SSL.assert_called_once_with('imap_server_address')
        mock_conn.login.assert_called_once_with('imap_user', 'imap_password')
        mock_conn.close.assert_called_once()
        mock_conn.logout.assert_called_once()

    @patch('airflow.contrib.hooks.imap_hook.imaplib')
    def test_has_mail_attachments_found(self, mock_imaplib):
        mock_conn = Mock(spec=imaplib.IMAP4_SSL)
        mock_imaplib.IMAP4_SSL.return_value = mock_conn
        mock_conn.login.return_value = ('OK', [])

        mock_conn.select.return_value = ('OK', [])
        mock_conn.search.return_value = ('OK', [b'1'])
        # TODO Add example mail with attachment test.txt
        mock_conn.fetch.return_value = ('OK', [(b'1 (RFC822 {123456}', b'body of the message', b')')])

        with ImapHook() as imap_hook:
            self.assertTrue(imap_hook.has_mail_attachments('test.txt', 'inbox', False))

    @patch('airflow.contrib.hooks.imap_hook.imaplib')
    def test_has_mail_attachments_not_found(self, mock_imaplib):
        mock_conn = Mock(spec=imaplib.IMAP4_SSL)
        mock_imaplib.IMAP4_SSL.return_value = mock_conn
        mock_conn.login.return_value = ('OK', [])

        mock_conn.select.return_value = ('OK', [])
        mock_conn.search.return_value = ('OK', [b'1'])
        mock_conn.fetch.return_value = ('OK', [(b'1 (RFC822 {123456}', b'body of the message', b')')])

        with ImapHook() as imap_hook:
            self.assertFalse(imap_hook.has_mail_attachments('test.txt', 'inbox', False))

    # TODO Add test_has_mail_attachments_with_regex_found
    # TODO Add test_has_mail_attachments_with_regex_not_found

    # TODO Add test_retrieve_mail_attachments_found
    # TODO Add test_retrieve_mail_attachments_not_found
    # TODO Add test_retrieve_mail_attachments_with_regex_found
    # TODO Add test_retrieve_mail_attachments_with_regex_not_found


if __name__ == '__main__':
    unittest.main()
