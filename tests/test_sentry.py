# -*- coding: utf-8 -*-
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
from unittest.mock import MagicMock, Mock

from freezegun import freeze_time
from sentry_sdk import configure_scope

from airflow.models import TaskInstance
from airflow.sentry import ConfiguredSentry
from airflow.settings import Session
from airflow.utils import timezone
from airflow.utils.state import State

EXECUTION_DATE = timezone.utcnow()
DAG_ID = "test_dag"
TASK_ID = "test_task"
OPERATOR = "test_operator"
TRY_NUMBER = 1
STATE = State.SUCCESS
DURATION = None
TEST_SCOPE = {
    "dag_id": DAG_ID,
    "task_id": TASK_ID,
    "execution_date": EXECUTION_DATE,
    "operator": OPERATOR,
    "try_number": TRY_NUMBER,
}
TASK_DATA = {
    "task_id": TASK_ID,
    "state": STATE,
    "operator": OPERATOR,
    "duration": DURATION,
}

CRUMB_DATE = datetime.datetime(2019, 5, 15)
CRUMB = {
    "timestamp": CRUMB_DATE,
    "type": "default",
    "category": "completed_tasks",
    "data": TASK_DATA,
    "level": "info",
}


class TestSentryHook(unittest.TestCase):
    def setUp(self):

        self.sentry = ConfiguredSentry()

        # Mock the Dag
        self.dag = Mock(dag_id=DAG_ID, params=[])
        self.dag.task_ids = [TASK_ID]

        # Mock the task
        self.task = Mock(dag=self.dag, dag_id=DAG_ID, task_id=TASK_ID, params=[])
        self.task.__class__.__name__ = OPERATOR

        self.ti = TaskInstance(self.task, execution_date=EXECUTION_DATE)
        self.ti.operator = OPERATOR
        self.ti.state = STATE

        self.dag.get_task_instances = MagicMock(return_value=[self.ti])

        self.session = Session()

    def test_add_tagging(self):
        """
        Test adding tags.
        """
        self.sentry.add_tagging(task_instance=self.ti)
        with configure_scope() as scope:
            for key, value in scope._tags.items():
                self.assertEqual(TEST_SCOPE[key], value)

    @freeze_time(CRUMB_DATE.isoformat())
    def test_add_breadcrumbs(self):
        """
        Test adding breadcrumbs.
        """
        self.sentry.add_tagging(task_instance=self.ti)
        self.sentry.add_breadcrumbs(task_instance=self.ti, session=self.session)

        with configure_scope() as scope:
            test_crumb = scope._breadcrumbs.pop()
            self.assertEqual(CRUMB, test_crumb)
