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
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.imap.sensors.imap_attachment import ImapAttachmentSensor


class TestImapAttachmentSensor:
    def setup_method(self):
        self.kwargs = dict(
            attachment_name="test_file",
            check_regex=False,
            mail_folder="INBOX",
            mail_filter="All",
            task_id="test_task",
            dag=None,
        )

    @pytest.mark.parametrize("has_attachment_return_value", [True, False])
    @patch("airflow.providers.imap.sensors.imap_attachment.ImapHook")
    def test_poke(self, mock_imap_hook, has_attachment_return_value):
        mock_imap_hook.return_value.__enter__ = Mock(return_value=mock_imap_hook)
        mock_imap_hook.has_mail_attachment.return_value = has_attachment_return_value

        has_attachment = ImapAttachmentSensor(**self.kwargs).poke(context={})

        assert has_attachment == mock_imap_hook.has_mail_attachment.return_value
        mock_imap_hook.has_mail_attachment.assert_called_once_with(
            name=self.kwargs["attachment_name"],
            check_regex=self.kwargs["check_regex"],
            mail_folder=self.kwargs["mail_folder"],
            mail_filter=self.kwargs["mail_filter"],
        )
