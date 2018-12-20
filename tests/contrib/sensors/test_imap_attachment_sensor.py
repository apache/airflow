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

import unittest

from mock import patch, Mock

from airflow import configuration
from airflow.contrib.sensors.imap_attachment_sensor import ImapAttachmentSensor
from airflow.models.connection import Connection
from airflow.utils import db

imap_hook_string = 'airflow.contrib.sensors.imap_attachment_sensor.ImapHook'


class TestImapAttachmentSensor(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            Connection(
                conn_id='imap_test',
                host='base_url',
                login='user',
                password='password'
            )
        )

    @patch(imap_hook_string)
    def test_poke_with_attachment_found(self, mock_imap_hook):
        mock_imap_hook.return_value.__enter__ = Mock(return_value=mock_imap_hook)
        mock_imap_hook.has_mail_attachment.return_value = True

        imap_attachment_sensor = ImapAttachmentSensor(
            conn_id='imap_test',
            attachment_name='test_attachment',
            task_id='check_for_attachment_on_mail_server_test',
            dag=None
        )

        self.assertTrue(imap_attachment_sensor.poke(context={}))
        mock_imap_hook.has_mail_attachment.assert_called_once_with(
            name='test_attachment',
            mail_folder='INBOX',
            check_regex=False
        )

    @patch(imap_hook_string)
    def test_poke_with_attachment_not_found(self, mock_imap_hook):
        mock_imap_hook.return_value.__enter__ = Mock(return_value=mock_imap_hook)
        mock_imap_hook.has_mail_attachment.return_value = False

        imap_attachment_sensor = ImapAttachmentSensor(
            conn_id='imap_test',
            attachment_name='test_attachment',
            task_id='check_for_attachment_on_mail_server_test',
            dag=None
        )

        self.assertFalse(imap_attachment_sensor.poke(context={}))
        mock_imap_hook.has_mail_attachment.assert_called_once_with(
            name='test_attachment',
            mail_folder='INBOX',
            check_regex=False
        )

    @patch(imap_hook_string)
    def test_poke_with_check_regex_true(self, mock_imap_hook):
        mock_imap_hook.return_value.__enter__ = Mock(return_value=mock_imap_hook)
        mock_imap_hook.has_mail_attachment.return_value = True

        imap_attachment_sensor = ImapAttachmentSensor(
            conn_id='imap_test',
            attachment_name='.*_test_attachment',
            task_id='check_for_attachment_on_mail_server_test',
            check_regex=True,
            dag=None
        )

        self.assertTrue(imap_attachment_sensor.poke(context={}))
        mock_imap_hook.has_mail_attachment.assert_called_once_with(
            name='.*_test_attachment',
            mail_folder='INBOX',
            check_regex=True
        )

    @patch(imap_hook_string)
    def test_poke_with_different_mail_folder(self, mock_imap_hook):
        mock_imap_hook.return_value.__enter__ = Mock(return_value=mock_imap_hook)
        mock_imap_hook.has_mail_attachment.return_value = True

        imap_attachment_sensor = ImapAttachmentSensor(
            conn_id='imap_test',
            attachment_name='test_attachment',
            task_id='check_for_attachment_on_mail_server_test',
            mail_folder='test',
            dag=None
        )

        self.assertTrue(imap_attachment_sensor.poke(context={}))
        mock_imap_hook.has_mail_attachment.assert_called_once_with(
            name='test_attachment',
            mail_folder='test',
            check_regex=False
        )


if __name__ == '__main__':
    unittest.main()
