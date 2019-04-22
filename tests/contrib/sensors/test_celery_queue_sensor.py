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
from mock import patch

from airflow.contrib.sensors.celery_queue_sensor import CeleryQueueSensor


class TestCeleryQueueSensor(unittest.TestCase):

    def setUp(self):
        class TestCeleryqueueSensor(CeleryQueueSensor):

            def _check_task_id(self, context):
                return True

        self.sensor = TestCeleryqueueSensor

    @patch('celery.app.control.Inspect')
    def test_poke_success(self, mock_inspect):
        mock_inspect_result = mock_inspect.return_value
        # test success
        mock_inspect_result.reserved.return_value = {
            'test_queue': []
        }

        mock_inspect_result.scheduled.return_value = {
            'test_queue': []
        }

        mock_inspect_result.active.return_value = {
            'test_queue': []
        }
        test_sensor = self.sensor(celery_queue='test_queue',
                                  task_id='test-task')
        self.assertTrue(test_sensor.poke(None))

    @patch('celery.app.control.Inspect')
    def test_poke_fail(self, mock_inspect):
        mock_inspect_result = mock_inspect.return_value
        # test success
        mock_inspect_result.reserved.return_value = {
            'test_queue': []
        }

        mock_inspect_result.scheduled.return_value = {
            'test_queue': []
        }

        mock_inspect_result.active.return_value = {
            'test_queue': ['task']
        }
        test_sensor = self.sensor(celery_queue='test_queue',
                                  task_id='test-task')
        self.assertFalse(test_sensor.poke(None))

    @patch('celery.app.control.Inspect')
    def test_poke_success_with_taskid(self, mock_inspect):
        test_sensor = self.sensor(celery_queue='test_queue',
                                  task_id='test-task',
                                  af_task_id='target-task')
        self.assertTrue(test_sensor.poke(None))
