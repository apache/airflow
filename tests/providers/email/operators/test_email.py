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

import datetime
import unittest
from unittest import mock

from airflow import DAG
from airflow.providers.email.operators.email import EmailOperator
from airflow.utils import timezone
from tests.test_utils.config import conf_vars

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
END_DATE = timezone.datetime(2016, 1, 2)
INTERVAL = datetime.timedelta(hours=12)
FROZEN_NOW = timezone.datetime(2016, 1, 2, 12, 1, 1)

send_email_test = mock.Mock()


class TestEmailOperator(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            'test_dag',
            default_args={
                'owner': 'airflow',
                'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL)
        self.addCleanup(self.dag.clear)

    def _run_as_operator(self, **kwargs):
        task = EmailOperator(
            to='airflow@example.com',
            subject='Test Run',
            html_content='The quick brown fox jumps over the lazy dog',
            task_id='task',
            dag=self.dag,
            **kwargs)
        task.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_execute(self):
        with conf_vars(
            {('email', 'email_backend'): 'tests.providers.email.operators.test_email.send_email_test'}
        ):
            self._run_as_operator()
        assert send_email_test.call_count == 1
