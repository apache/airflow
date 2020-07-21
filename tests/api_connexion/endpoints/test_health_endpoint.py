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
from datetime import timedelta

from airflow.jobs.base_job import BaseJob
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.www import app

HEALTHY = "healthy"
UNHEALTHY = "unhealthy"


class TestHealthTestBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        with create_session() as session:
            session.query(BaseJob).delete()

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(BaseJob).delete()


class TestGetHeath(TestHealthTestBase):
    @provide_session
    def test_healthy_scheduler_status(self, session):
        last_scheduler_heartbeat_for_testing_1 = timezone.utcnow()
        session.add(
            BaseJob(
                job_type="SchedulerJob",
                state=State.RUNNING,
                latest_heartbeat=last_scheduler_heartbeat_for_testing_1,
            )
        )
        session.commit()
        resp_json = self.client.get("/api/v1/health").json
        self.assertEqual("healthy", resp_json["metadatabase"]["status"])
        self.assertEqual("healthy", resp_json["scheduler"]["status"])
        self.assertEqual(
            last_scheduler_heartbeat_for_testing_1.isoformat(),
            resp_json["scheduler"]["latest_scheduler_heartbeat"],
        )

    @provide_session
    def test_unhealthy_scheduler_is_slow(self, session):
        last_scheduler_heartbeat_for_testing_2 = timezone.utcnow() - timedelta(
            minutes=1
        )
        session.add(
            BaseJob(
                job_type="SchedulerJob",
                state=State.RUNNING,
                latest_heartbeat=last_scheduler_heartbeat_for_testing_2,
            )
        )
        session.commit()
        resp_json = self.client.get("/api/v1/health").json
        self.assertEqual("healthy", resp_json["metadatabase"]["status"])
        self.assertEqual("unhealthy", resp_json["scheduler"]["status"])
        self.assertEqual(
            last_scheduler_heartbeat_for_testing_2.isoformat(),
            resp_json["scheduler"]["latest_scheduler_heartbeat"],
        )

    def test_unhealthy_scheduler_no_job(self):
        resp_json = self.client.get("/api/v1/health").json
        self.assertEqual("healthy", resp_json["metadatabase"]["status"])
        self.assertEqual("unhealthy", resp_json["scheduler"]["status"])
        self.assertIsNone(None, resp_json["scheduler"]["latest_scheduler_heartbeat"])
