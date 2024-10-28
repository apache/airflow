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

import boto3
import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.ses import SesHook

boto3.setup_default_session()


@mock_aws
def test_get_conn():
    hook = SesHook(aws_conn_id="aws_default")
    assert hook.get_conn() is not None


@mock_aws
@pytest.mark.parametrize(
    "to",
    [
        "to@domain.com",
        ["to1@domain.com", "to2@domain.com"],
        "to1@domain.com,to2@domain.com",
    ],
)
@pytest.mark.parametrize(
    "cc",
    [
        "cc@domain.com",
        ["cc1@domain.com", "cc2@domain.com"],
        "cc1@domain.com,cc2@domain.com",
    ],
)
@pytest.mark.parametrize(
    "bcc",
    [
        "bcc@domain.com",
        ["bcc1@domain.com", "bcc2@domain.com"],
        "bcc1@domain.com,bcc2@domain.com",
    ],
)
def test_send_email(to, cc, bcc):
    # Given
    hook = SesHook()
    ses_client = hook.get_conn()
    mail_from = "test_from@domain.com"

    # Amazon only allows to send emails from verified addresses,
    # then we need to validate the from address before sending the email,
    # otherwise this test would raise a `botocore.errorfactory.MessageRejected` exception
    ses_client.verify_email_identity(EmailAddress=mail_from)

    # When
    response = hook.send_email(
        mail_from=mail_from,
        to=to,
        subject="subject",
        html_content="<html>Test</html>",
        cc=cc,
        bcc=bcc,
        reply_to="reply_to@domain.com",
        return_path="return_path@domain.com",
    )

    # Then
    assert response is not None
    assert isinstance(response, dict)
    assert "MessageId" in response
