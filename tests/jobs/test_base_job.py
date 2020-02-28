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

from mock import Mock, patch
from sqlalchemy.exc import OperationalError

from airflow.jobs.base_job import BaseJob
from airflow.utils import configuration, timezone
from airflow.utils.session import create_session
from airflow.utils.state import State


class TestBaseJob(unittest.TestCase):
    class TestJob(BaseJob):
        __mapper_args__ = {
            'polymorphic_identity': 'TestJob'
        }

        def __init__(self, func, **kwargs):
            self.func = func
            super().__init__(**kwargs)

        def _execute(self):
            return self.func()

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
            session.add(old_job)
            session.flush()
            old_job.update_heartbeat(old_job.get_heartbeat() - datetime.timedelta(seconds=20))
            job = self.TestJob(None, heartrate=10)
            session.add(job)
            session.merge(old_job)
            session.flush()

            self.assertEqual(
                self.TestJob.most_recent_job(session=session),
                job
            )

            session.rollback()

    def test_is_alive(self):
        job = self.TestJob(None, heartrate=10, state=State.RUNNING)
        self.assertTrue(job.is_alive())

        job.update_heartbeat(timezone.utcnow() - datetime.timedelta(seconds=20))
        self.assertTrue(job.is_alive())

        job.update_heartbeat(timezone.utcnow() - datetime.timedelta(seconds=21))
        self.assertFalse(job.is_alive())

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        self.assertFalse(job.is_alive())

        job.state = State.SUCCESS
        job.update_heartbeat(timezone.utcnow() - datetime.timedelta(seconds=10))
        self.assertFalse(job.is_alive(), "Completed jobs even with recent heartbeat should not be alive")

    @patch('airflow.jobs.base_job.create_session')
    def test_heartbeat_failed(self, mock_create_session):
        when = timezone.utcnow() - datetime.timedelta(seconds=60)
        with create_session() as session:
            mock_session = Mock(spec_set=session, name="MockSession")
            mock_create_session.return_value.__enter__.return_value = mock_session

            job = self.TestJob(None, heartrate=10, state=State.RUNNING)
            session.add(job)
            session.commit()
            job.update_heartbeat(heartbeat_time=when)

            mock_session.commit.side_effect = OperationalError("Force fail", {}, None)

            job.heartbeat()

            job = session.query(BaseJob).filter(BaseJob.id == job.id).first()

            self.assertEqual(job.get_heartbeat(), when, "attribute not updated when heartbeat fails")

    def test_update_heartbeat(self):
        with create_session() as session:
            job = self.TestJob(None)
            session.add(job)
            session.flush()

            old_heartbeat = job.get_heartbeat()
            new_heartbeat_expected = old_heartbeat + datetime.timedelta(seconds=10)
            job.update_heartbeat(heartbeat_time=new_heartbeat_expected, session=session)
            new_heartbeat_actual = job.get_heartbeat()

            self.assertEqual(new_heartbeat_expected, new_heartbeat_actual)

    def test_update_heartbeat_redis(self):
        try:
            BaseJob.redis_enabled = True
            with patch('airflow.jobs.base_job.BaseJob.redis') as mocked_redis:
                with create_session() as session:
                    job = self.TestJob(None)
                    session.add(job)
                    session.flush()

                    heartbeat = timezone.utcnow()
                    heartbeat_in_seconds = (heartbeat - timezone.utc_epoch()).total_seconds()

                    job.update_heartbeat(heartbeat_time=heartbeat)
                    Mock.assert_called_with(mocked_redis.zadd,
                                            'TestJob',
                                            {str(job.id): str(heartbeat_in_seconds)})
        finally:
            BaseJob.redis_enabled = False

    def test_get_heartbeat_initial(self):
        with create_session() as session:
            job = self.TestJob(None)
            session.add(job)
            session.flush()

            self.assertIsNotNone(job.get_heartbeat())

    def test_get_heartbeat_initial_redis(self):
        with patch.object(configuration.conf, 'getboolean', return_value=True):
            with patch('airflow.jobs.base_job.BaseJob.redis') as mocked_redis:
                with create_session() as session:
                    job = self.TestJob(None)
                    session.add(job)
                    session.flush()

                    mocked_redis.zscore.return_value = None

                    self.assertIsNotNone(job.get_heartbeat())
