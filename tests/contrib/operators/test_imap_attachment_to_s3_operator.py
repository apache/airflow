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

from mock import patch

from airflow.contrib.operators.imap_attachment_to_s3_operator import ImapAttachmentToS3Operator


class TestImapAttachmentToS3Operator(unittest.TestCase):

    def setUp(self):
        self.kwargs = dict(
            imap_attachment_name='test_file',
            s3_key='test_file',
            imap_mail_folder='INBOX',
            imap_check_regex=False,
            s3_overwrite=False,
            task_id='test_task',
            dag=None
        )

    @patch('airflow.contrib.operators.imap_attachment_to_s3_operator.S3Hook')
    @patch('airflow.contrib.operators.imap_attachment_to_s3_operator.ImapHook')
    def test_execute(self, mock_imap_hook, mock_s3_hook):
        mock_imap_hook.return_value.__enter__ = mock_imap_hook
        mock_imap_hook.return_value.retrieve_mail_attachments.return_value = [('test_file', b'Hello World')]

        ImapAttachmentToS3Operator(**self.kwargs).execute(context={})

        mock_imap_hook.return_value.retrieve_mail_attachments.assert_called_once_with(
            name=self.kwargs['imap_attachment_name'],
            mail_folder=self.kwargs['imap_mail_folder'],
            check_regex=self.kwargs['imap_check_regex'],
            latest_only=True
        )
        mock_s3_hook.return_value.load_string.assert_called_once_with(
            string_data=mock_imap_hook.return_value.retrieve_mail_attachments.return_value[0][1],
            key=self.kwargs['s3_key']
        )
