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

from unittest import mock

import pytest

from airflow.providers.microsoft.azure.emailer import send_email
from airflow.providers.microsoft.azure.hooks.msgraph import MSGraphMailHook

FROM_EMAIL = "airflow@example.com"
TO_EMAIL = "user@example.com"


@mock.patch("airflow.providers.microsoft.azure.emailer.MSGraphMailHook", autospec=True)
class TestSendEmail:
    def test_send_email(self, mock_hook):
        send_email(
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            conn_id="msgraph_api",
            from_email=FROM_EMAIL,
        )

        mock_hook.assert_called_once_with(conn_id="msgraph_api")
        mock_hook.return_value.send_email.assert_called_once_with(
            from_email=FROM_EMAIL,
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            files=None,
            cc=None,
            bcc=None,
            custom_headers=None,
        )

    def test_send_email_without_conn_id(self, mock_hook):
        mock_hook.default_conn_name = MSGraphMailHook.default_conn_name

        send_email(
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            conn_id=None,
            from_email=FROM_EMAIL,
        )

        mock_hook.assert_called_once_with(conn_id="msgraph_default")

    def test_send_email_without_from_email(self, mock_hook):
        with pytest.raises(ValueError, match="`from_email` configuration has to be set"):
            send_email(to=TO_EMAIL, subject="Airflow alert", html_content="Something happened")

        mock_hook.assert_not_called()

    def test_send_email_when_dryrun(self, mock_hook):
        send_email(
            to=TO_EMAIL,
            subject="Airflow alert",
            html_content="Something happened",
            dryrun=True,
            from_email=FROM_EMAIL,
        )

        mock_hook.assert_not_called()
