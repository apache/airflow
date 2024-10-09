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

import pytest

from airflow.decorators import task
from airflow.models import Connection
from airflow.utils import timezone
from airflow.utils.types import NOTSET
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields

DEFAULT_DATE = timezone.datetime(2023, 1, 1)


@pytest.mark.db_test
class TestEmailDecorator:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.dag_maker = dag_maker

        with dag_maker(dag_id="email_deco_dag") as dag:
            ...

        self.dag = dag

    def teardown_method(self):
        clear_db_runs()
        clear_db_dags()
        clear_rendered_ti_fields()

    def execute_task(self, task):
        session = self.dag_maker.session
        dag_run = self.dag_maker.create_dagrun(
            run_id=f"email_deco_test_{DEFAULT_DATE.date()}", session=session
        )
        ti = dag_run.get_task_instance(task.operator.task_id, session=session)
        return_val = task.operator.execute(context={"ti": ti})

        return ti, return_val

    def test_email_decorator_init(self):
        """Test the initialization of the @task.email decorator."""

        with self.dag:

            @task.email(to="joe@apache.org", subject="testing")
            def email(): ...

            email_task = email()

        assert email_task.operator.task_id == "email"
        assert email_task.operator.html_content == NOTSET
        assert email_task.operator.to == "joe@apache.org"
        assert email_task.operator.subject == "testing"
        assert email_task.operator.from_email is None
        assert email_task.operator.files == []
        assert email_task.operator.cc is None
        assert email_task.operator.bcc is None
        assert email_task.operator.mime_subtype == "mixed"
        assert email_task.operator.mime_charset == "utf-8"
        assert email_task.operator.custom_headers is None

    @patch("airflow.providers.smtp.hooks.smtp.SmtpHook.get_connection")
    @patch("airflow.providers.smtp.hooks.smtp.smtplib")
    def test_op_args_kwargs(self, mock_smtplib, mock_hook_conn):
        """Test op_args and op_kwargs are passed to the decorator."""

        mock_hook_conn.return_value = Connection(
            conn_id="smtp_default",
            conn_type="smtp",
            host="smtp_server_address",
            login="smtp_user",
            password="smtp_password",
            port=465,
            extra=json.dumps(dict(from_email="sender")),
        )

        with self.dag:

            @task.email(to="joe@apache.org", subject="testing")
            def email(id, other):
                return f"Hello {id}, goodbye {other}"

            email_task = email("world", other="joe")

        assert email_task.operator.html_content == NOTSET

        ti, _ = self.execute_task(email_task)

        assert email_task.operator.html_content == "Hello world, goodbye joe"
