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

import tempfile
from dataclasses import dataclass
from unittest import mock
from unittest.mock import AsyncMock

import pytest

from airflow.providers.smtp.notifications.smtp import SmtpNotifier, send_smtp_notification

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

TRY_NUMBER = 0

SMTP_CONN_ID = "smtp_default"
SMTP_AUTH_TYPE = "oauth2"

# Standard settings
DEFAULT_EMAIL_PARAMS = {
    "mime_subtype": "mixed",
    "mime_charset": "utf-8",
    "files": None,
    "cc": None,
    "bcc": None,
    "custom_headers": None,
}

# DAG settings
TEST_DAG_ID = "test_dag"
TEST_TASK_ID = "test_task"
TEST_TASK_STATE = None
TEST_RUN_ID = "test_run"

# Jinja template patterns
DAG_ID_TEMPLATE_STRING = "{{dag.dag_id}}"
TI_TEMPLATE_STRING = "{{ti.task_id}}"

SENDER_EMAIL_SUFFIX = "sender@test.com"
RECEIVER_EMAIL_SUFFIX = "receiver@test.com"
# Base test values
TEST_SENDER = f"test_{SENDER_EMAIL_SUFFIX}"
TEST_RECEIVER = f"test_{RECEIVER_EMAIL_SUFFIX}"
TEST_SUBJECT = "subject"
TEST_BODY = "body"

NOTIFIER_DEFAULT_PARAMS = {
    "from_email": TEST_SENDER,
    "to": TEST_RECEIVER,
    "subject": TEST_SUBJECT,
    "html_content": TEST_BODY,
}


# Templated versions
@dataclass(frozen=True)
class TemplatedString:
    template: str

    @property
    def rendered(self) -> str:
        return self.template.replace(DAG_ID_TEMPLATE_STRING, TEST_DAG_ID).replace(
            TI_TEMPLATE_STRING, TEST_TASK_ID
        )


# DAG-based templates
TEMPLATED_SENDER = TemplatedString(f"{DAG_ID_TEMPLATE_STRING}_{SENDER_EMAIL_SUFFIX}")
TEMPLATED_RECEIVER = TemplatedString(f"{DAG_ID_TEMPLATE_STRING}_{RECEIVER_EMAIL_SUFFIX}")
TEMPLATED_SUBJECT = TemplatedString(f"{TEST_SUBJECT} {DAG_ID_TEMPLATE_STRING}")
TEMPLATED_BODY = TemplatedString(f"{TEST_BODY} {DAG_ID_TEMPLATE_STRING}")

# Task-based templates
TEMPLATED_TI_SUBJECT = TemplatedString(f"{TEST_SUBJECT} {TI_TEMPLATE_STRING}")
TEMPLATED_TI_SENDER = TemplatedString(f"{TI_TEMPLATE_STRING}_{SENDER_EMAIL_SUFFIX}")


class TestSmtpNotifier:
    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier(_self, mock_smtphook_hook, create_dag_without_db):
        notifier = send_smtp_notification(**NOTIFIER_DEFAULT_PARAMS)
        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            **NOTIFIER_DEFAULT_PARAMS,
            smtp_conn_id=SMTP_CONN_ID,
            **DEFAULT_EMAIL_PARAMS,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_notifier_class(self, mock_smtphook_hook, create_dag_without_db):
        notifier = SmtpNotifier(**NOTIFIER_DEFAULT_PARAMS)
        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email=TEST_SENDER,
            to=TEST_RECEIVER,
            subject=TEST_SUBJECT,
            html_content=TEST_BODY,
            smtp_conn_id=SMTP_CONN_ID,
            **DEFAULT_EMAIL_PARAMS,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_templated(self, mock_smtphook_hook, create_dag_without_db):
        notifier = SmtpNotifier(
            from_email=TEMPLATED_SENDER.template,
            to=TEMPLATED_RECEIVER.template,
            subject=TEMPLATED_SUBJECT.template,
            html_content=TEMPLATED_BODY.template,
        )
        context = {"dag": create_dag_without_db(TEST_DAG_ID)}

        notifier(context)

        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email=TEMPLATED_SENDER.rendered,
            to=TEMPLATED_RECEIVER.rendered,
            subject=TEMPLATED_SUBJECT.rendered,
            html_content=TEMPLATED_BODY.rendered,
            smtp_conn_id=SMTP_CONN_ID,
            **DEFAULT_EMAIL_PARAMS,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_defaults(self, mock_smtphook_hook, create_dag_without_db, mock_task_instance):
        # TODO: we can use create_runtime_ti fixture in place of mock_task_instance once provider has minimum AF to Airflow 3.0+
        mock_ti = mock_task_instance(
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            try_number=TRY_NUMBER,
            max_tries=0,
            state=TEST_TASK_STATE,
        )

        context = {"dag": create_dag_without_db(TEST_DAG_ID), "ti": mock_ti}
        notifier = SmtpNotifier(
            from_email=TEST_SENDER,
            to=TEST_RECEIVER,
        )
        mock_smtphook_hook.return_value.__enter__.return_value.subject_template = None
        mock_smtphook_hook.return_value.__enter__.return_value.html_content_template = None

        notifier(context)

        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email=TEST_SENDER,
            to=TEST_RECEIVER,
            subject=f"DAG {TEST_DAG_ID} - Task {TEST_TASK_ID} - Run ID {TEST_RUN_ID} in State {TEST_TASK_STATE}",
            html_content=mock.ANY,
            smtp_conn_id=SMTP_CONN_ID,
            **DEFAULT_EMAIL_PARAMS,
        )
        content = mock_smtphook_hook.return_value.__enter__().send_email_smtp.call_args.kwargs["html_content"]
        assert f"{TRY_NUMBER} of 1" in content

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_nondefault_connection_extra(
        self, mock_smtphook_hook, create_dag_without_db, mock_task_instance
    ):
        # TODO: we can use create_runtime_ti fixture in place of mock_task_instance once provider has minimum AF to Airflow 3.0+
        ti = mock_task_instance(
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            try_number=TRY_NUMBER,
            max_tries=0,
            state=TEST_TASK_STATE,
        )
        context = {"dag": create_dag_without_db(TEST_DAG_ID), "ti": ti}

        with (
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_subject,
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_content,
        ):
            f_subject.write(TEMPLATED_TI_SUBJECT.template)
            f_subject.flush()

            f_content.write(TEST_BODY)
            f_content.flush()
            mock_smtphook_hook.return_value.__enter__.return_value.from_email = TEMPLATED_TI_SENDER.template
            mock_smtphook_hook.return_value.__enter__.return_value.subject_template = f_subject.name
            mock_smtphook_hook.return_value.__enter__.return_value.html_content_template = f_content.name

            notifier = SmtpNotifier(to=TEST_RECEIVER)
            notifier(context)

            mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
                from_email=TEMPLATED_TI_SENDER.rendered,
                to=TEST_RECEIVER,
                subject=TEMPLATED_TI_SUBJECT.rendered,
                html_content=TEST_BODY,
                smtp_conn_id=SMTP_CONN_ID,
                **DEFAULT_EMAIL_PARAMS,
            )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_oauth2_passes_auth_type(self, mock_smtphook_hook, create_dag_without_db):
        notifier = SmtpNotifier(**NOTIFIER_DEFAULT_PARAMS, auth_type=SMTP_AUTH_TYPE)

        notifier({"dag": create_dag_without_db(TEST_DAG_ID)})

        mock_smtphook_hook.assert_called_once_with(smtp_conn_id=SMTP_CONN_ID, auth_type=SMTP_AUTH_TYPE)


@pytest.mark.asyncio
@pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Async support was added to BaseNotifier in 3.1.0")
class TestSmtpNotifierAsync:
    @pytest.fixture
    def mock_smtp_client(self):
        """Create a mock SMTP object with async capabilities."""
        mock_smtp = AsyncMock()
        mock_smtp.asend_email_smtp = AsyncMock()
        return mock_smtp

    @pytest.fixture
    def mock_smtp_hook(self, mock_smtp_client):
        """Set up the SMTP hook with async context manager."""
        with mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook") as mock_hook:
            mock_hook.return_value.__aenter__ = AsyncMock(return_value=mock_smtp_client)
            yield mock_hook

    @pytest.mark.asyncio
    async def test_async_notifier(self, mock_smtp_hook, mock_smtp_client, create_dag_without_db):
        notifier = SmtpNotifier(
            **NOTIFIER_DEFAULT_PARAMS, context={"dag": create_dag_without_db(TEST_DAG_ID)}
        )
        await notifier.async_notify({"dag": create_dag_without_db(TEST_DAG_ID)})

        mock_smtp_client.asend_email_smtp.assert_called_once_with(
            smtp_conn_id=SMTP_CONN_ID,
            **NOTIFIER_DEFAULT_PARAMS,
            **DEFAULT_EMAIL_PARAMS,
        )

    async def test_async_notifier_with_notifier_class(
        self, mock_smtp_hook, mock_smtp_client, create_dag_without_db
    ):
        notifier = SmtpNotifier(
            **NOTIFIER_DEFAULT_PARAMS, context={"dag": create_dag_without_db(TEST_DAG_ID)}
        )

        await notifier

        mock_smtp_client.asend_email_smtp.assert_called_once_with(
            smtp_conn_id=SMTP_CONN_ID,
            **NOTIFIER_DEFAULT_PARAMS,
            **DEFAULT_EMAIL_PARAMS,
        )

    async def test_async_notifier_templated(self, mock_smtp_hook, mock_smtp_client, create_dag_without_db):
        notifier = SmtpNotifier(
            from_email=TEMPLATED_SENDER.template,
            to=TEMPLATED_RECEIVER.template,
            subject=TEMPLATED_SUBJECT.template,
            html_content=TEMPLATED_BODY.template,
            context={"dag": create_dag_without_db(TEST_DAG_ID)},
        )

        await notifier

        mock_smtp_client.asend_email_smtp.assert_called_once_with(
            smtp_conn_id=SMTP_CONN_ID,
            from_email=TEMPLATED_SENDER.rendered,
            to=TEMPLATED_RECEIVER.rendered,
            subject=TEMPLATED_SUBJECT.rendered,
            html_content=TEMPLATED_BODY.rendered,
            **DEFAULT_EMAIL_PARAMS,
        )

    async def test_async_notifier_with_defaults(
        self, mock_smtp_hook, mock_smtp_client, create_dag_without_db, mock_task_instance
    ):
        mock_smtp_client.subject_template = None
        mock_smtp_client.html_content_template = None
        mock_smtp_client.from_email = None

        notifier = SmtpNotifier(
            **NOTIFIER_DEFAULT_PARAMS, context={"dag": create_dag_without_db(TEST_DAG_ID)}
        )

        await notifier

        mock_smtp_client.asend_email_smtp.assert_called_once_with(
            smtp_conn_id=SMTP_CONN_ID,
            **NOTIFIER_DEFAULT_PARAMS,
            **DEFAULT_EMAIL_PARAMS,
        )

    async def test_async_notifier_with_nondefault_connection_extra(
        self, mock_smtp_hook, mock_smtp_client, create_dag_without_db, mock_task_instance
    ):
        with (
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_subject,
            tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_content,
        ):
            f_subject.write(TEST_SUBJECT)
            f_subject.flush()

            f_content.write(TEST_BODY)
            f_content.flush()

            mock_smtp_client.from_email = TEST_SENDER
            mock_smtp_client.subject_template = f_subject.name
            mock_smtp_client.html_content_template = f_content.name

            notifier = SmtpNotifier(to=TEST_RECEIVER, context={"dag": create_dag_without_db(TEST_DAG_ID)})

            await notifier

            mock_smtp_client.asend_email_smtp.assert_called_once_with(
                smtp_conn_id=SMTP_CONN_ID,
                from_email=TEST_SENDER,
                to=TEST_RECEIVER,
                subject=TEST_SUBJECT,
                html_content=TEST_BODY,
                **DEFAULT_EMAIL_PARAMS,
            )
