# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import requests
import requests_mock
import unittest

from airflow.exceptions import AirflowException
from airflow.hooks.druid_hook import DruidHook


class TestDruidHook(unittest.TestCase):

    def setUp(self):
        super(TestDruidHook, self).setUp()

        session = requests.Session()
        adapter = requests_mock.Adapter()
        session.mount('mock', adapter)

    @requests_mock.mock()
    def test_submit_gone_wrong(self, m):
        hook = DruidHook()
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}'
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "FAILED"}}'
        )

        # The job failed for some reason
        with self.assertRaises(AirflowException):
            hook.submit_indexing_job('Long json file')

        self.assertTrue(task_post.called_once)
        self.assertTrue(status_check.called_once)

    @requests_mock.mock()
    def test_submit_ok(self, m):
        hook = DruidHook()
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}'
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "SUCCESS"}}'
        )

        # Exists just as it should
        hook.submit_indexing_job('Long json file')

        self.assertTrue(task_post.called_once)
        self.assertTrue(status_check.called_once)

    @requests_mock.mock()
    def test_submit_unknown_response(self, m):
        hook = DruidHook()
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}'
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "UNKNOWN"}}'
        )

        # An unknown error code
        with self.assertRaises(AirflowException):
            hook.submit_indexing_job('Long json file')

        self.assertTrue(task_post.called_once)
        self.assertTrue(status_check.called_once)

    @requests_mock.mock()
    def test_submit_timeout(self, m):
        hook = DruidHook(timeout=0, max_ingestion_time=5)
        task_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}'
        )
        status_check = m.get(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/status',
            text='{"status":{"status": "RUNNING"}}'
        )
        shutdown_post = m.post(
            'http://druid-overlord:8081/druid/indexer/v1/task/9f8a7359-77d4-4612-b0cd-cc2f6a3c28de/shutdown',
            text='{"task":"9f8a7359-77d4-4612-b0cd-cc2f6a3c28de"}'
        )

        # Because the jobs keeps running
        with self.assertRaises(AirflowException):
            hook.submit_indexing_job('Long json file')

        self.assertTrue(task_post.called_once)
        self.assertTrue(status_check.called)
        self.assertTrue(shutdown_post.called_once)





