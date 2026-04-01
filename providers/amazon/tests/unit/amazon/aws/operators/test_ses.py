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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.operators.ses import SesEmailOperator

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

TASK_ID = "ses_email_job"
AWS_CONN_ID = "custom_aws_conn"
MAIL_FROM = "sender@example.com"
TO = ["recipient@example.com"]
SUBJECT = "Test Subject"
HTML_CONTENT = "<h1>Test Email</h1><p>This is a test.</p>"


class TestSesEmailOperator:
    @pytest.fixture(autouse=True)
    def _setup_test_cases(self):
        self.default_op_kwargs = {
            "task_id": TASK_ID,
            "mail_from": MAIL_FROM,
            "to": TO,
            "subject": SUBJECT,
            "html_content": HTML_CONTENT,
        }

    def test_init(self):
        """Test operator initialization with default parameters."""
        op = SesEmailOperator(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.mail_from == MAIL_FROM
        assert op.to == TO
        assert op.subject == SUBJECT
        assert op.html_content == HTML_CONTENT

    def test_init_with_custom_params(self):
        """Test operator initialization with custom AWS parameters."""
        op = SesEmailOperator(
            **self.default_op_kwargs,
            aws_conn_id=AWS_CONN_ID,
            region_name="us-west-1",
            verify="/spam/egg.pem",
            botocore_config={"read_timeout": 42},
            cc=["cc@example.com"],
            bcc=["bcc@example.com"],
            reply_to="reply@example.com",
            return_path="return@example.com",
        )
        assert op.hook.aws_conn_id == AWS_CONN_ID
        assert op.hook._region_name == "us-west-1"
        assert op.hook._verify == "/spam/egg.pem"
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42
        assert op.cc == ["cc@example.com"]
        assert op.bcc == ["bcc@example.com"]
        assert op.reply_to == "reply@example.com"
        assert op.return_path == "return@example.com"

    @mock.patch.object(SesEmailOperator, "hook")
    def test_execute_basic(self, mocked_hook):
        """Test basic email sending execution."""
        hook_response = {"MessageId": "test-message-id-123"}
        mocked_hook.send_email.return_value = hook_response

        op = SesEmailOperator(**self.default_op_kwargs)
        result = op.execute({})

        assert result == hook_response
        mocked_hook.send_email.assert_called_once_with(
            mail_from=MAIL_FROM,
            to=TO,
            subject=SUBJECT,
            html_content=HTML_CONTENT,
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            reply_to=None,
            return_path=None,
            custom_headers=None,
        )

    @mock.patch.object(SesEmailOperator, "hook")
    def test_execute_with_all_params(self, mocked_hook):
        """Test email sending with all optional parameters."""
        hook_response = {"MessageId": "test-message-id-456"}
        mocked_hook.send_email.return_value = hook_response

        custom_headers = {"X-Custom-Header": "CustomValue"}
        files = ["/path/to/file1.txt", "/path/to/file2.pdf"]

        op = SesEmailOperator(
            **self.default_op_kwargs,
            cc=["cc@example.com"],
            bcc=["bcc@example.com"],
            files=files,
            mime_subtype="alternative",
            mime_charset="iso-8859-1",
            reply_to="reply@example.com",
            return_path="return@example.com",
            custom_headers=custom_headers,
        )
        result = op.execute({})

        assert result == hook_response
        mocked_hook.send_email.assert_called_once_with(
            mail_from=MAIL_FROM,
            to=TO,
            subject=SUBJECT,
            html_content=HTML_CONTENT,
            files=files,
            cc=["cc@example.com"],
            bcc=["bcc@example.com"],
            mime_subtype="alternative",
            mime_charset="iso-8859-1",
            reply_to="reply@example.com",
            return_path="return@example.com",
            custom_headers=custom_headers,
        )

    @mock.patch.object(SesEmailOperator, "hook")
    def test_execute_with_string_to(self, mocked_hook):
        """Test email sending with 'to' as a string instead of list."""
        hook_response = {"MessageId": "test-message-id-789"}
        mocked_hook.send_email.return_value = hook_response

        op = SesEmailOperator(
            task_id=TASK_ID,
            mail_from=MAIL_FROM,
            to="single@example.com",
            subject=SUBJECT,
            html_content=HTML_CONTENT,
        )
        result = op.execute({})

        assert result == hook_response
        mocked_hook.send_email.assert_called_once_with(
            mail_from=MAIL_FROM,
            to="single@example.com",
            subject=SUBJECT,
            html_content=HTML_CONTENT,
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            reply_to=None,
            return_path=None,
            custom_headers=None,
        )

    def test_template_fields(self):
        """Test that template fields are properly configured."""
        operator = SesEmailOperator(**self.default_op_kwargs)
        validate_template_fields(operator)
