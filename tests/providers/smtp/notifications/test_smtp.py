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
from unittest import mock

import pytest

from airflow.configuration import conf
from airflow.models import SlaMiss
from airflow.operators.empty import EmptyOperator
from airflow.providers.smtp.hooks.smtp import SmtpHook
from airflow.providers.smtp.notifications.smtp import (
    SmtpNotifier,
    send_smtp_notification,
)
from airflow.utils import timezone
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

SMTP_API_DEFAULT_CONN_ID = SmtpHook.default_conn_name


class TestSmtpNotifier:
    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = send_smtp_notification(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
        )
        notifier({"dag": dag})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_notifier_class(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        notifier = SmtpNotifier(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
        )
        notifier({"dag": dag})
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com",
            to="test_reciver@test.com",
            subject="subject",
            html_content="body",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_templated(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")

        notifier = SmtpNotifier(
            from_email="test_sender@test.com {{dag.dag_id}}",
            to="test_reciver@test.com {{dag.dag_id}}",
            subject="subject {{dag.dag_id}}",
            html_content="body {{dag.dag_id}}",
        )
        context = {"dag": dag}
        notifier(context)
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email="test_sender@test.com test_notifier",
            to="test_reciver@test.com test_notifier",
            subject="subject test_notifier",
            html_content="body test_notifier",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_defaults(self, mock_smtphook_hook, create_task_instance):
        ti = create_task_instance(dag_id="dag", task_id="op", execution_date=timezone.datetime(2018, 1, 1))
        context = {"dag": ti.dag_run.dag, "ti": ti}
        notifier = SmtpNotifier(
            from_email=conf.get("smtp", "smtp_mail_from"),
            to="test_reciver@test.com",
        )
        notifier(context)
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email=conf.get("smtp", "smtp_mail_from"),
            to="test_reciver@test.com",
            subject="DAG dag - Task op - Run ID test in State None",
            html_content="""<!DOCTYPE html>\n<html>\n    <head>\n        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n        <meta name="viewport" content="width=device-width">\n    </head>\n<body>\n    <table role="presentation">\n        \n        <tr>\n            <td>Run ID:</td>\n            <td>test</td>\n        </tr>\n        <tr>\n            <td>Try:</td>\n            <td>0 of 1</td>\n        </tr>\n        <tr>\n            <td>Task State:</td>\n            <td>None</td>\n        </tr>\n        <tr>\n            <td>Host:</td>\n            <td></td>\n        </tr>\n        <tr>\n            <td>Log Link:</td>\n            <td><a href="http://localhost:8080/dags/dag/grid?dag_run_id=test&task_id=op&map_index=-1&tab=logs" style="text-decoration:underline;">http://localhost:8080/dags/dag/grid?dag_run_id=test&task_id=op&map_index=-1&tab=logs</a></td>\n        </tr>\n        <tr>\n            <td>Mark Success Link:</td>\n            <td><a href="http://localhost:8080/confirm?task_id=op&dag_id=dag&dag_run_id=test&upstream=false&downstream=false&state=success" style="text-decoration:underline;">http://localhost:8080/confirm?task_id=op&dag_id=dag&dag_run_id=test&upstream=false&downstream=false&state=success</a></td>\n        </tr>\n        \n    </table>\n</body>\n</html>""",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_defaults_sla(self, mock_smtphook_hook, dag_maker):
        with dag_maker("test_notifier") as dag:
            EmptyOperator(task_id="task1")
        context = {
            "dag": dag,
            "slas": [SlaMiss(task_id="op", dag_id=dag.dag_id, execution_date=timezone.datetime(2018, 1, 1))],
            "task_list": [],
            "blocking_task_list": [],
            "blocking_tis": [],
        }
        notifier = SmtpNotifier(
            from_email=conf.get("smtp", "smtp_mail_from"),
            to="test_reciver@test.com",
        )
        notifier(context)
        mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
            from_email=conf.get("smtp", "smtp_mail_from"),
            to="test_reciver@test.com",
            subject="SLA Missed for DAG test_notifier - Task op",
            html_content="""<!DOCTYPE html>\n<html>\n    <head>\n        <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />\n        <meta name="viewport" content="width=device-width">\n    </head>\n<body>\n    <table role="presentation">\n        \n        <tr>\n            <td>Dag:</td>\n            <td>test_notifier</td>\n        </tr>\n        <tr>\n            <td>Task List:</td>\n            <td>[]</td>\n        </tr>\n        <tr>\n            <td>Blocking Task List:</td>\n            <td>[]</td>\n        </tr>\n        <tr>\n            <td>SLAs:</td>\n            <td>[(\'test_notifier\', \'op\', \'2018-01-01T00:00:00+00:00\')]</td>\n        </tr>\n        <tr>\n            <td>Blocking TI\'s</td>\n            <td>[]</td>\n        </tr>\n        \n    </table>\n</body>\n</html>""",
            smtp_conn_id="smtp_default",
            files=None,
            cc=None,
            bcc=None,
            mime_subtype="mixed",
            mime_charset="utf-8",
            custom_headers=None,
        )

    @mock.patch("airflow.providers.smtp.notifications.smtp.SmtpHook")
    def test_notifier_with_nondefault_conf_vars(self, mock_smtphook_hook, create_task_instance):
        ti = create_task_instance(dag_id="dag", task_id="op", execution_date=timezone.datetime(2018, 1, 1))
        context = {"dag": ti.dag_run.dag, "ti": ti}

        with tempfile.NamedTemporaryFile(mode="wt", suffix=".txt") as f_subject, tempfile.NamedTemporaryFile(
            mode="wt", suffix=".txt"
        ) as f_content:
            f_subject.write("Task {{ ti.task_id }} failed")
            f_subject.flush()

            f_content.write("Mock content goes here")
            f_content.flush()

            with conf_vars(
                {
                    ("smtp", "templated_html_content_path"): f_content.name,
                    ("smtp", "templated_email_subject_path"): f_subject.name,
                }
            ):
                notifier = SmtpNotifier(
                    from_email=conf.get("smtp", "smtp_mail_from"),
                    to="test_reciver@test.com",
                )
                notifier(context)
                mock_smtphook_hook.return_value.__enter__().send_email_smtp.assert_called_once_with(
                    from_email=conf.get("smtp", "smtp_mail_from"),
                    to="test_reciver@test.com",
                    subject="Task op failed",
                    html_content="Mock content goes here",
                    smtp_conn_id="smtp_default",
                    files=None,
                    cc=None,
                    bcc=None,
                    mime_subtype="mixed",
                    mime_charset="utf-8",
                    custom_headers=None,
                )
