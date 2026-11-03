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

import os
import subprocess
import sys
from unittest import mock

import boto3
import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.ses import SesHook, _build_mime_message

boto3.setup_default_session()


TEST_TO_ADDRESSES = [
    pytest.param("to@domain.com", id="to=single_string"),
    pytest.param("to1@domain.com,to2@domain.com", id="to=comma_string"),
    pytest.param(["to1@domain.com", "to2@domain.com"], id="to=list"),
]

TEST_CC_ADDRESSES = [
    pytest.param("cc@domain.com", id="cc=single_string"),
    pytest.param("cc1@domain.com,cc2@domain.com", id="cc=comma_string"),
    pytest.param(["cc1@domain.com", "cc2@domain.com"], id="cc=list"),
]

TEST_BCC_ADDRESSES = [
    pytest.param("bcc@domain.com", id="bcc=single_string"),
    pytest.param("bcc1@domain.com,bcc2@domain.com", id="bcc=comma_string"),
    pytest.param(["bcc1@domain.com", "bcc2@domain.com"], id="bcc=list"),
]

TEST_FROM_ADDRESS = "test_from@domain.com"
TEST_SUBJECT = "subject"
TEST_HTML_CONTENT = "<html>Test</html>"
TEST_REPLY_TO = "reply_to@domain.com"
TEST_RETURN_PATH = "return_path@domain.com"


@mock_aws
def _verify_address(address: str) -> None:
    """
    Amazon only allows emails from verified addresses. If the address is not verified,
    this test will raise `botocore.errorfactory.MessageRejected`.
    """
    SesHook().get_conn().verify_email_identity(EmailAddress=address)


@mock_aws
class TestSesHook:
    def test_get_conn(self):
        hook = SesHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    @pytest.mark.parametrize("to", TEST_TO_ADDRESSES)
    @pytest.mark.parametrize("cc", TEST_CC_ADDRESSES)
    @pytest.mark.parametrize("bcc", TEST_BCC_ADDRESSES)
    def test_send_email(self, to, cc, bcc):
        _verify_address(TEST_FROM_ADDRESS)
        hook = SesHook()

        response = hook.send_email(
            mail_from=TEST_FROM_ADDRESS,
            to=to,
            subject=TEST_SUBJECT,
            html_content=TEST_HTML_CONTENT,
            cc=cc,
            bcc=bcc,
            reply_to=TEST_REPLY_TO,
            return_path=TEST_RETURN_PATH,
        )

        assert response is not None
        assert isinstance(response, dict)
        assert "MessageId" in response


@pytest.mark.asyncio
class TestAsyncSesHook:
    """The mock_aws decorator uses `moto` which does not currently support async SES so we mock it manually."""

    @pytest.fixture
    def mock_async_client(self):
        return mock.AsyncMock()

    @pytest.fixture
    def mock_get_async_conn(self, mock_async_client):
        with mock.patch.object(SesHook, "get_async_conn") as mocked_conn:
            mocked_conn.return_value.__aenter__.return_value = mock_async_client
            yield mocked_conn

    async def test_get_async_conn(self, mock_get_async_conn, mock_async_client):
        hook = SesHook()
        async with await hook.get_async_conn() as async_conn:
            assert async_conn is mock_async_client

    @pytest.mark.parametrize("to", TEST_TO_ADDRESSES)
    @pytest.mark.parametrize("cc", TEST_CC_ADDRESSES)
    @pytest.mark.parametrize("bcc", TEST_BCC_ADDRESSES)
    async def test_asend_email(self, mock_get_async_conn, mock_async_client, to, cc, bcc):
        _verify_address(TEST_FROM_ADDRESS)
        hook = SesHook()

        mock_async_client.send_raw_email.return_value = {"MessageId": "test_message_id"}

        response = await hook.asend_email(
            mail_from=TEST_FROM_ADDRESS,
            to=to,
            subject=TEST_SUBJECT,
            html_content=TEST_HTML_CONTENT,
            cc=cc,
            bcc=bcc,
            reply_to=TEST_REPLY_TO,
            return_path=TEST_RETURN_PATH,
        )

        assert response is not None
        assert isinstance(response, dict)
        assert "MessageId" in response
        assert response["MessageId"] == "test_message_id"

        mock_async_client.send_raw_email.assert_called_once()


class TestBuildMimeMessage:
    def test_message_and_recipients(self, tmp_path):
        attachment = tmp_path / "report.txt"
        attachment.write_bytes(b"data")

        msg, recipients = _build_mime_message(
            mail_from=TEST_FROM_ADDRESS,
            to="to1@domain.com; to2@domain.com",
            subject=TEST_SUBJECT,
            html_content=TEST_HTML_CONTENT,
            files=[os.fspath(attachment)],
            cc=["cc@domain.com"],
            bcc="bcc@domain.com",
            custom_headers={"Reply-To": TEST_REPLY_TO},
        )

        assert recipients == ["to1@domain.com", "to2@domain.com", "cc@domain.com", "bcc@domain.com"]
        assert msg["From"] == TEST_FROM_ADDRESS
        assert msg["To"] == "to1@domain.com, to2@domain.com"
        assert msg["CC"] == "cc@domain.com"
        assert "Bcc" not in msg
        assert msg["Subject"] == TEST_SUBJECT
        assert msg["Reply-To"] == TEST_REPLY_TO
        assert "Date" in msg
        html_part, attachment_part = msg.get_payload()
        assert html_part.get_payload(decode=True) == TEST_HTML_CONTENT.encode()
        assert attachment_part.get_filename() == "report.txt"
        assert attachment_part["Content-ID"] == "<report.txt>"
        assert attachment_part.get_payload(decode=True) == b"data"

    def test_without_sender_omits_from_header(self):
        msg, _ = _build_mime_message(
            mail_from=None, to="to@domain.com", subject=TEST_SUBJECT, html_content=TEST_HTML_CONTENT
        )

        assert "From" not in msg

    @pytest.mark.parametrize(
        ("to", "match"),
        [
            pytest.param(1, "Unexpected argument type: Received 'int'", id="not_iterable"),
            pytest.param(["to@domain.com", 2], "The items in your iterable must be strings", id="not_string"),
        ],
    )
    def test_rejects_non_string_addresses(self, to, match):
        with pytest.raises(TypeError, match=match):
            _build_mime_message(
                mail_from=TEST_FROM_ADDRESS, to=to, subject=TEST_SUBJECT, html_content=TEST_HTML_CONTENT
            )


def test_hook_imports_without_core_email_utils():
    """``airflow.utils.email`` is moving to the SMTP provider, so the hook must not import it."""
    script = (
        "import sys\n"
        "sys.modules['airflow.utils.email'] = None\n"
        "import airflow.providers.amazon.aws.hooks.ses\n"
    )
    subprocess.run([sys.executable, "-c", script], check=True)
