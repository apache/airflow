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

import json
from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.smtp.operators.smtp import EmailOperator

smtplib_string = "airflow.providers.smtp.hooks.smtp.smtplib"


class TestEmailOperator:
    @patch("airflow.providers.smtp.hooks.smtp.SmtpHook.get_connection")
    @patch(smtplib_string)
    def test_loading_sender_email_from_connection(self, mock_smtplib, mock_hook_conn):
        """Check if the EmailOperator is able to load the sender email from the smtp connection."""
        custom_retry_limit = 10
        custom_timeout = 60
        sender_email = "sender_email"
        mock_hook_conn.return_value = Connection(
            conn_id="mock_conn",
            conn_type="smtp",
            host="smtp_server_address",
            login="smtp_user",
            password="smtp_password",
            port=465,
            extra=json.dumps(
                dict(from_email=sender_email, timeout=custom_timeout, retry_limit=custom_retry_limit)
            ),
        )
        smtp_client_mock = mock_smtplib.SMTP_SSL()
        op = EmailOperator(task_id="test_email", to="to", subject="subject", html_content="content")
        op.execute({})
        call_args = smtp_client_mock.sendmail.call_args[1]
        assert call_args["from_addr"] == sender_email
