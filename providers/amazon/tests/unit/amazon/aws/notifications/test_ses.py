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

from airflow.providers.amazon.aws.notifications.ses import SesNotifier, send_ses_notification
from airflow.providers.amazon.version_compat import NOTSET

TEST_EMAIL_PARAMS = {
    "mail_from": "from@test.com",
    "to": "to@test.com",
    "subject": "Test Subject",
    "html_content": "<p>Test Content</p>",
}

# The hook sets these default values if they are not provided
HOOK_DEFAULTS = {
    "mime_charset": "utf-8",
    "mime_subtype": "mixed",
}


class TestSesNotifier:
    def test_class_and_notifier_are_same(self):
        assert send_ses_notification is SesNotifier

    @pytest.mark.parametrize(
        "aws_conn_id",
        [
            pytest.param("aws_test_conn_id", id="custom-conn"),
            pytest.param(None, id="none-conn"),
            pytest.param(NOTSET, id="default-value"),
        ],
    )
    @pytest.mark.parametrize(
        "region_name",
        [
            pytest.param("eu-west-2", id="custom-region"),
            pytest.param(None, id="no-region"),
            pytest.param(NOTSET, id="default-value"),
        ],
    )
    def test_parameters_propagate_to_hook(self, aws_conn_id, region_name):
        """Test notifier attributes propagate to SesHook."""
        notifier_kwargs = {}
        if aws_conn_id is not NOTSET:
            notifier_kwargs["aws_conn_id"] = aws_conn_id
        if region_name is not NOTSET:
            notifier_kwargs["region_name"] = region_name

        notifier = SesNotifier(**notifier_kwargs, **TEST_EMAIL_PARAMS)
        with mock.patch("airflow.providers.amazon.aws.notifications.ses.SesHook") as mock_hook:
            hook = notifier.hook
            assert hook is notifier.hook, "Hook property not cached"
            mock_hook.assert_called_once_with(
                aws_conn_id=(aws_conn_id if aws_conn_id is not NOTSET else "aws_default"),
                region_name=(region_name if region_name is not NOTSET else None),
            )

            # Basic check for notifier
            notifier.notify({})
            mock_hook.return_value.send_email.assert_called_once_with(**TEST_EMAIL_PARAMS, **HOOK_DEFAULTS)

    @pytest.mark.asyncio
    async def test_async_notify(self):
        """Test async notification sends correctly."""
        notifier = SesNotifier(**TEST_EMAIL_PARAMS)
        with mock.patch("airflow.providers.amazon.aws.notifications.ses.SesHook") as mock_hook:
            mock_hook.return_value.asend_email = mock.AsyncMock()

            await notifier.async_notify({})

            mock_hook.return_value.asend_email.assert_called_once_with(**TEST_EMAIL_PARAMS, **HOOK_DEFAULTS)

    def test_ses_notifier_with_optional_params(self):
        """Test notifier handles all optional parameters correctly."""
        email_params = {
            **TEST_EMAIL_PARAMS,
            "files": ["test.txt"],
            "cc": ["cc@test.com"],
            "bcc": ["bcc@test.com"],
            "mime_subtype": "alternative",
            "mime_charset": "ascii",
            "reply_to": "reply@test.com",
            "return_path": "bounce@test.com",
            "custom_headers": {"X-Custom": "value"},
        }

        notifier = SesNotifier(**email_params)
        with mock.patch("airflow.providers.amazon.aws.notifications.ses.SesHook") as mock_hook:
            notifier.notify({})

            mock_hook.return_value.send_email.assert_called_once_with(**email_params)

    def test_ses_notifier_templated(self, create_dag_without_db):
        """Test template fields are properly rendered."""
        templated_params = {
            "aws_conn_id": "{{ dag.dag_id }}",
            "region_name": "{{ var_region }}",
            "mail_from": "{{ var_from }}",
            "to": "{{ var_to }}",
            "subject": "{{ var_subject }}",
            "html_content": "Hello {{ var_name }}",
            "cc": ["cc@{{ var_domain }}"],
            "bcc": ["bcc@{{ var_domain }}"],
            "reply_to": "reply@{{ var_domain }}",
        }

        notifier = SesNotifier(**templated_params)
        with mock.patch("airflow.providers.amazon.aws.notifications.ses.SesHook") as mock_hook:
            context = {
                "dag": create_dag_without_db("test_ses_notifier_templated"),
                "var_region": "us-west-1",
                "var_from": "from@example.com",
                "var_to": "to@example.com",
                "var_subject": "Test Email",
                "var_name": "John",
                "var_domain": "example.com",
            }
            notifier(context)

            mock_hook.assert_called_once_with(
                aws_conn_id="test_ses_notifier_templated",
                region_name="us-west-1",
            )
            mock_hook.return_value.send_email.assert_called_once_with(
                mail_from="from@example.com",
                to="to@example.com",
                subject="Test Email",
                html_content="Hello John",
                cc=["cc@example.com"],
                bcc=["bcc@example.com"],
                mime_subtype="mixed",
                mime_charset="utf-8",
                reply_to="reply@example.com",
            )
