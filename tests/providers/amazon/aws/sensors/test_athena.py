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
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.athena import AWSAthenaHook
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor


class TestAthenaSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = AthenaSensor(
            task_id='test_athena_sensor',
            query_execution_id='abc',
            sleep_time=5,
            max_retries=1,
            aws_conn_id='aws_default',
        )

    @mock.patch.object(AWSAthenaHook, 'poll_query_status', side_effect=("SUCCEEDED",))
    def test_poke_success(self, mock_poll_query_status):
        self.assertTrue(self.sensor.poke(None))

    @mock.patch.object(AWSAthenaHook, 'poll_query_status', side_effect=("RUNNING",))
    def test_poke_running(self, mock_poll_query_status):
        self.assertFalse(self.sensor.poke(None))

    @mock.patch.object(AWSAthenaHook, 'poll_query_status', side_effect=("QUEUED",))
    def test_poke_queued(self, mock_poll_query_status):
        self.assertFalse(self.sensor.poke(None))

    @mock.patch.object(AWSAthenaHook, 'poll_query_status', side_effect=("FAILED",))
    def test_poke_failed(self, mock_poll_query_status):
        with self.assertRaises(AirflowException) as context:
            self.sensor.poke(None)
        self.assertIn('Athena sensor failed', str(context.exception))

    @mock.patch.object(AWSAthenaHook, 'poll_query_status', side_effect=("CANCELLED",))
    def test_poke_cancelled(self, mock_poll_query_status):
        with self.assertRaises(AirflowException) as context:
            self.sensor.poke(None)
        self.assertIn('Athena sensor failed', str(context.exception))
