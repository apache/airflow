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

import datetime
from unittest import mock

import pytest

from airflow.models.dag import DAG
from airflow.operators.email import EmailOperator
from airflow.utils import timezone
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

send_email_test = mock.Mock()


class TestEmailOperator:
    def setup_class(self):
        self.dag = DAG(
            "test_dag",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
        )

    def _run_as_operator(self, **kwargs):
        task = EmailOperator(
            to="airflow@example.com",
            subject="Test Run",
            html_content="The quick brown fox jumps over the lazy dog",
            task_id="task",
            dag=self.dag,
            files=["/tmp/Report-A-{{ ds }}.csv"],
            custom_headers={"Reply-To": "reply_to@example.com"},
            **kwargs,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.dag.clear()

    def _run_as_operator_with_from(self, **kwargs):
        task = EmailOperator(
            to="airflow@example.com",
            subject="Test Run",
            html_content="The quick brown fox jumps over the lazy dog",
            task_id="task2",
            dag=self.dag,
            files=["/tmp/Report-A-{{ ds }}.csv"],
            custom_headers={"Reply-To": "reply_to@example.com"},
            from_email="noreply@example.com",
            **kwargs,
        )
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.dag.clear()

    def test_execute(self):
        with conf_vars(
            {
                ("email", "email_backend"): "tests.operators.test_email.send_email_test",
                ("email", "from_email"): "noreply-config@example.com",
            }
        ):
            self._run_as_operator()
        assert send_email_test.call_count == 1
        call_args = send_email_test.call_args.kwargs
        assert call_args["files"] == ["/tmp/Report-A-2016-01-01.csv"]
        assert call_args["custom_headers"] == {"Reply-To": "reply_to@example.com"}
        # When there is no email provided, it should get the from_email from config as usual
        assert call_args["from_email"] == "noreply-config@example.com"

        # Add test for from email
        with conf_vars(
            {
                ("email", "email_backend"): "tests.operators.test_email.send_email_test",
                ("email", "from_email"): "noreply-config@example.com",
            }
        ):
            self._run_as_operator_with_from()
        assert send_email_test.call_count == 2
        call_args = send_email_test.call_args.kwargs
        # When Email is provided, it should pick the provided email
        assert call_args["from_email"] == "noreply@example.com"
        assert call_args["from_email"] != "noreply-config@example.com"
