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

import unittest
import datetime
import difflib

from airflow import settings, models
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.state import State
from airflow.utils import asciiart, sla, timezone


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class SlaEmailTest(unittest.TestCase):
    """
    Test that the email helper functions produce correct copy.
    """
    def assertTextMatch(self, a, b):
        """
        Replace AssertionErrors with a nice diff.
        """
        try:
            self.assertEqual(a, b)
        except AssertionError:
            raise AssertionError("Text mismatch! Diff: \n{}".format(
                "\n".join(difflib.unified_diff(a.split("\n"), b.split("\n")))))

    def setUp(self):
        """
        Provide a test DAG, with a root task with three children.
        """
        # Get a db session
        session = settings.Session()

        # Create a test DAG and store it in the db.
        self.test_dag = models.DAG(
            dag_id="{}_dag".format(self._testMethodName),
            start_date=DEFAULT_DATE,
        )

        orm_dag = models.DagModel(dag_id=self.test_dag.dag_id)
        orm_dag.is_paused = False
        session.merge(orm_dag)

        # Create tasks and task instances in the test DAG
        with self.test_dag:
            self.test_task = DummyOperator(task_id="test_task",
                                           email="ops_team@example.org")

            self.test_ti = models.TaskInstance(task=self.test_task,
                                               execution_date=DEFAULT_DATE,
                                               state=State.RUNNING)
            session.merge(self.test_ti)

            for i in range(3):
                downstream_task = DummyOperator(
                    task_id="downstream_task_{}".format(i),
                    email="dev_team@example.org")
                self.test_task >> downstream_task
                session.merge(models.TaskInstance(task=downstream_task,
                                                  execution_date=DEFAULT_DATE,
                                                  state=State.NONE))

        # Save created DAG/TIs
        session.commit()

    def test_send_task_duration_exceeded_email(self):
        context = {
            "ti": self.test_ti,
            "task": self.test_task,
            "dag": self.test_dag
        }

        # Test with just an expected duration set.
        self.test_task.expected_duration = datetime.timedelta(hours=1)

        email_to, email_subject, email_body = \
            sla.send_task_duration_exceeded_email(context)

        self.assertEqual(
            sorted(email_to),
            sorted(["ops_team@example.org", "dev_team@example.org"])
        )

        self.assertEqual(
            email_subject,
            "[airflow] [SLA] Exceeded duration on "
            "test_send_task_duration_exceeded_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]",
        )

        desired_body = (
            "<pre><code>test_send_task_duration_exceeded_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]</pre></code> missed an SLA: duration "
            "exceeded <pre><code>1:00:00</pre></code>.\n\n"

            # TODO: this is likely too brittle
            "View Task Details: http://localhost:8080/admin/airflow/task?task_id=test_task&dag_id=test_send_task_duration_exceeded_email_dag&execution_date=2016-01-01T00%3A00%3A00%2B00%3A00\n\n" ## noqa

            "This may be impacting the following downstream tasks:\n"
            "<pre><code>"
            "test_send_task_duration_exceeded_email_dag.downstream_task_0 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_duration_exceeded_email_dag.downstream_task_1 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_duration_exceeded_email_dag.downstream_task_2 "
            "[2016-01-01 00:00:00+00:00]\n"
            "{art}"
            "</pre></code>".format(art=asciiart.snail)
        )
        self.assertTextMatch(email_body, desired_body)

    def test_send_task_late_start_email(self):
        context = {
            "ti": self.test_ti,
            "task": self.test_task,
            "dag": self.test_dag
        }

        # Test with just an expected start set.
        self.test_task.expected_start = datetime.timedelta(hours=1)

        email_to, email_subject, email_body = \
            sla.send_task_late_start_email(context)

        self.assertEqual(
            sorted(email_to),
            sorted(["ops_team@example.org", "dev_team@example.org"])
        )

        self.assertEqual(
            email_subject,
            "[airflow] [SLA] Late start on "
            "test_send_task_late_start_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]",
        )

        desired_body = (
            "<pre><code>test_send_task_late_start_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]</pre></code> missed an SLA: did not "
            "start by <pre><code>2016-01-01 01:00:00+00:00</pre></code>.\n\n"

            # TODO: this is likely too brittle
            "View Task Details: http://localhost:8080/admin/airflow/task?task_id=test_task&dag_id=test_send_task_late_start_email_dag&execution_date=2016-01-01T00%3A00%3A00%2B00%3A00\n\n" ## noqa

            "This may be impacting the following downstream tasks:\n"
            "<pre><code>"
            "test_send_task_late_start_email_dag.downstream_task_0 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_late_start_email_dag.downstream_task_1 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_late_start_email_dag.downstream_task_2 "
            "[2016-01-01 00:00:00+00:00]\n"
            "{art}"
            "</pre></code>".format(art=asciiart.snail)
        )
        self.assertTextMatch(email_body, desired_body)

    def test_send_task_late_finish_email(self):
        context = {
            "ti": self.test_ti,
            "task": self.test_task,
            "dag": self.test_dag
        }

        # Test with just an expected finish set.
        self.test_task.expected_finish = datetime.timedelta(hours=1)

        email_to, email_subject, email_body = \
            sla.send_task_late_finish_email(context)

        self.assertEqual(
            sorted(email_to),
            sorted(["ops_team@example.org", "dev_team@example.org"])
        )

        self.assertEqual(
            email_subject,
            "[airflow] [SLA] Late finish on "
            "test_send_task_late_finish_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]",
        )

        desired_body = (
            "<pre><code>test_send_task_late_finish_email_dag.test_task "
            "[2016-01-01 00:00:00+00:00]</pre></code> missed an SLA: did not "
            "finish by <pre><code>2016-01-01 01:00:00+00:00</pre></code>.\n\n"

            # TODO: this is likely too brittle
            "View Task Details: http://localhost:8080/admin/airflow/task?task_id=test_task&dag_id=test_send_task_late_finish_email_dag&execution_date=2016-01-01T00%3A00%3A00%2B00%3A00\n\n" ## noqa

            "This may be impacting the following downstream tasks:\n"
            "<pre><code>"
            "test_send_task_late_finish_email_dag.downstream_task_0 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_late_finish_email_dag.downstream_task_1 "
            "[2016-01-01 00:00:00+00:00]\n"
            "test_send_task_late_finish_email_dag.downstream_task_2 "
            "[2016-01-01 00:00:00+00:00]\n"
            "{art}"
            "</pre></code>".format(art=asciiart.snail)
        )
        self.assertTextMatch(email_body, desired_body)
