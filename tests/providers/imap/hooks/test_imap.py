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

import imaplib
import unittest
from unittest.mock import Mock, mock_open, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.imap.hooks.imap import ImapHook
from airflow.utils import db

imaplib_string = 'airflow.providers.imap.hooks.imap.imaplib'
open_string = 'airflow.providers.imap.hooks.imap.open'


def _create_fake_imap(mock_imaplib, with_mail=False, attachment_name='test1.csv'):
    mock_conn = Mock(spec=imaplib.IMAP4_SSL)
    mock_imaplib.IMAP4_SSL.return_value = mock_conn

    mock_conn.login.return_value = ('OK', [])

    if with_mail:
        mock_conn.select.return_value = ('OK', [])
        mock_conn.search.return_value = ('OK', [b'1'])
        mail_string = (
            'Content-Type: multipart/mixed; boundary=123\r\n--123\r\n'
            'Content-Disposition: attachment; filename="{}";'
            'Content-Transfer-Encoding: base64\r\nSWQsTmFtZQoxLEZlbGl4\r\n--123--'.format(attachment_name)
        )
        mock_conn.fetch.return_value = ('OK', [(b'', mail_string.encode('utf-8'))])
        mock_conn.close.return_value = ('OK', [])

    mock_conn.logout.return_value = ('OK', [])

    return mock_conn


class TestImapHook(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='imap_default',
                conn_type='imap',
                host='imap_server_address',
                login='imap_user',
                password='imap_password',
            )
        )

    @patch(imaplib_string)
    def test_connect_and_disconnect(self, mock_imaplib):
        mock_conn = _create_fake_imap(mock_imaplib)

        with ImapHook():
            pass

        mock_imaplib.IMAP4_SSL.assert_called_once_with('imap_server_address')
        mock_conn.login.assert_called_once_with('imap_user', 'imap_password')  # pylint: disable=no-member
        assert mock_conn.logout.call_count == 1

    @patch(imaplib_string)
    def test_has_mail_attachment_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment('test1.csv')

        assert has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment('test1.txt')

        assert not has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_regex_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment(name=r'test(\d+).csv', check_regex=True)

        assert has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_regex_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            has_attachment_in_inbox = imap_hook.has_mail_attachment(name=r'test_(\d+).csv', check_regex=True)

        assert not has_attachment_in_inbox

    @patch(imaplib_string)
    def test_has_mail_attachment_with_mail_filter(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.has_mail_attachment(name='test1.csv', mail_filter=mail_filter)

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments('test1.csv')

        assert attachments_in_inbox == [('test1.csv', b'SWQsTmFtZQoxLEZlbGl4')]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.retrieve_mail_attachments('test1.txt')

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_regex_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments(
                name=r'test(\d+).csv', check_regex=True
            )

        assert attachments_in_inbox == [('test1.csv', b'SWQsTmFtZQoxLEZlbGl4')]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_regex_not_found(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.retrieve_mail_attachments(
                    name=r'test_(\d+).csv',
                    check_regex=True,
                )

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_latest_only(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            attachments_in_inbox = imap_hook.retrieve_mail_attachments(name='test1.csv', latest_only=True)

        assert attachments_in_inbox == [('test1.csv', b'SWQsTmFtZQoxLEZlbGl4')]

    @patch(imaplib_string)
    def test_retrieve_mail_attachments_with_mail_filter(self, mock_imaplib):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.retrieve_mail_attachments(name='test1.csv', mail_filter=mail_filter)

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments('test1.csv', 'test_directory')

        mock_open_method.assert_called_once_with('test_directory/test1.csv', 'wb')
        mock_open_method.return_value.write.assert_called_once_with(b'SWQsTmFtZQoxLEZlbGl4')

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_not_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.download_mail_attachments('test1.txt', 'test_directory')

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_regex_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name=r'test(\d+).csv', local_output_directory='test_directory', check_regex=True
            )

        mock_open_method.assert_called_once_with('test_directory/test1.csv', 'wb')
        mock_open_method.return_value.write.assert_called_once_with(b'SWQsTmFtZQoxLEZlbGl4')

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_regex_not_found(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            with pytest.raises(AirflowException):
                imap_hook.download_mail_attachments(
                    name=r'test_(\d+).csv',
                    local_output_directory='test_directory',
                    check_regex=True,
                )

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_latest_only(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name='test1.csv', local_output_directory='test_directory', latest_only=True
            )

        mock_open_method.assert_called_once_with('test_directory/test1.csv', 'wb')
        mock_open_method.return_value.write.assert_called_once_with(b'SWQsTmFtZQoxLEZlbGl4')

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_escaping_chars(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True, attachment_name='../test1.csv')

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(name='../test1.csv', local_output_directory='test_directory')

        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch('airflow.providers.imap.hooks.imap.os.path.islink', return_value=True)
    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_symlink(self, mock_imaplib, mock_open_method, mock_is_symlink):
        _create_fake_imap(mock_imaplib, with_mail=True, attachment_name='symlink')

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(name='symlink', local_output_directory='test_directory')

        assert mock_is_symlink.call_count == 1
        mock_open_method.assert_not_called()
        mock_open_method.return_value.write.assert_not_called()

    @patch(open_string, new_callable=mock_open)
    @patch(imaplib_string)
    def test_download_mail_attachments_with_mail_filter(self, mock_imaplib, mock_open_method):
        _create_fake_imap(mock_imaplib, with_mail=True)
        mail_filter = '(SINCE "01-Jan-2019")'

        with ImapHook() as imap_hook:
            imap_hook.download_mail_attachments(
                name='test1.csv', local_output_directory='test_directory', mail_filter=mail_filter
            )

        mock_imaplib.IMAP4_SSL.return_value.search.assert_called_once_with(None, mail_filter)
        assert mock_open_method.call_count == 1
