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
#

from unittest import mock

from airflow.providers.amazon.aws.utils.emailer import send_email


@mock.patch("airflow.providers.amazon.aws.utils.emailer.SESHook")
def test_send_email(mock_hook):
    send_email(
        to="to@test.com",
        subject="subject",
        html_content="content",
    )
    mock_hook.return_value.send_email.assert_called_once_with(
        mail_from=None,
        to="to@test.com",
        subject="subject",
        html_content="content",
        bcc=None,
        cc=None,
        files=None,
        mime_charset="utf-8",
        mime_subtype="mixed",
    )
