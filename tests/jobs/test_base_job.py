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
#

import datetime
import unittest

from sqlalchemy.exc import OperationalError

from airflow.jobs import BaseJob
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from tests.compat import Mock, patch
from tests.test_utils.config import conf_vars


class BaseJobTest(unittest.TestCase):
    class TestJob(BaseJob):
        __mapper_args__ = {
            'polymorphic_identity': 'TestJob'
        }

        def __init__(self, cb, **kwargs):
            self.cb = cb
            super(BaseJobTest.TestJob, self).__init__(**kwargs)

        def _execute(self):
            return self.cb()

    def test_state_success(self):
        job = self.TestJob(lambda: True)
        job.run()

        self.assertEqual(job.state, State.SUCCESS)
        self.assertIsNotNone(job.end_date)

    def test_state_sysexit(self):
        import sys
        job = self.TestJob(lambda: sys.exit(0))
        job.run()

        self.assertEqual(job.state, State.SUCCESS)
        self.assertIsNotNone(job.end_date)

    def test_state_failed(self):
        def abort():
            raise RuntimeError("fail")

        job = self.TestJob(abort)
        with self.assertRaises(RuntimeError):
            job.run()

        self.assertEqual(job.state, State.FAILED)
        self.assertIsNotNone(job.end_date)

    def test_most_recent_job(self):

        with create_session() as session:
            old_job = self.TestJob(None, heartrate=10)
            old_job.latest_heartbeat = old_job.latest_heartbeat - datetime.timedelta(seconds=20)
            job = self.TestJob(None, heartrate=10)
            session.add(job)
            session.add(old_job)
            session.flush()

            self.assertEqual(
                self.TestJob.most_recent_job(session=session),
                job
            )

            session.rollback()

    def test_is_alive(self):
        job = self.TestJob(None, heartrate=10, state=State.RUNNING)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=21)
        self.assertFalse(job.is_alive())

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        self.assertFalse(job.is_alive())

        job.state = State.SUCCESS
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        self.assertFalse(job.is_alive(), "Completed jobs even with recent heartbeat should not be alive")

    @patch('airflow.jobs.base_job.create_session')
    def test_heartbeat_failed(self, mock_create_session):
        when = timezone.utcnow() - datetime.timedelta(seconds=60)
        with create_session() as session:
            mock_session = Mock(spec_set=session, name="MockSession")
            mock_create_session.return_value.__enter__.return_value = mock_session

            job = self.TestJob(None, heartrate=10, state=State.RUNNING)
            job.latest_heartbeat = when

            mock_session.commit.side_effect = OperationalError("Force fail", {}, None)

            job.heartbeat()

            self.assertEqual(job.latest_heartbeat, when, "attribute not updated when heartbeat fails")

    @conf_vars({('scheduler', 'max_tis_per_query'): '100'})
    @patch('airflow.jobs.base_job.executors.get_default_executor')
    @patch('airflow.jobs.base_job.get_hostname')
    @patch('airflow.jobs.base_job.getpass.getuser')
    def test_essential_attr(self, mock_getuser, mock_hostname, mock_default_executor):
        mock_sequential_executor = SequentialExecutor()
        mock_hostname.return_value = "test_hostname"
        mock_getuser.return_value = "testuser"
        mock_default_executor.return_value = mock_sequential_executor

        test_job = self.TestJob(None, heartrate=10, dag_id="example_dag", state=State.RUNNING)
        self.assertEqual(test_job.executor_class, "SequentialExecutor")
        self.assertEqual(test_job.heartrate, 10)
        self.assertEqual(test_job.dag_id, "example_dag")
        self.assertEqual(test_job.hostname, "test_hostname")
        self.assertEqual(test_job.max_tis_per_query, 100)
        self.assertEqual(test_job.unixname, "testuser")
        self.assertEqual(test_job.state, "running")
        self.assertEqual(test_job.executor, mock_sequential_executor)
