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

from email.mime.multipart import MIMEMultipart
from unittest import mock

import pytest

from airflow.providers.smtp.utils import emailer
from airflow.utils import email
from airflow.utils.deprecation_tools import DeprecatedImportWarning


@pytest.mark.parametrize(
    "name",
    ["send_email", "send_email_smtp", "build_mime_message", "send_mime_email", "get_email_address_list"],
)
def test_deprecated_names_resolve_to_provider(name):
    with pytest.warns(DeprecatedImportWarning, match=f"airflow.providers.smtp.utils.emailer.{name}"):
        assert getattr(email, name) is getattr(emailer, name)


@pytest.mark.db_test
class TestDeprecatedSendFromServerProcess:
    """Email sent from a server process, such as a listener in the API server, must find a DB Connection."""

    @pytest.fixture(autouse=True)
    def _db_only_connection(self, monkeypatch):
        from airflow.models import Connection
        from airflow.utils.session import create_session

        from tests_common.test_utils.db import clear_db_connections

        monkeypatch.delenv("AIRFLOW_CONN_SMTP_DB_ONLY", raising=False)
        monkeypatch.setenv("_AIRFLOW_PROCESS_CONTEXT", "server")
        clear_db_connections(add_default_connections_back=False)
        with create_session() as session:
            session.add(
                Connection(conn_id="smtp_db_only", conn_type="smtp", login="db-user", password="db-pass")
            )
        yield
        clear_db_connections()

    @mock.patch("smtplib.SMTP", autospec=True)
    def test_send_mime_email_uses_db_connection(self, mock_smtp):
        with pytest.warns(DeprecatedImportWarning):
            send_mime_email = email.send_mime_email

        send_mime_email("from@example.com", ["to@example.com"], MIMEMultipart(), conn_id="smtp_db_only")

        mock_smtp.return_value.login.assert_called_once_with("db-user", "db-pass")
        assert mock_smtp.return_value.sendmail.called
