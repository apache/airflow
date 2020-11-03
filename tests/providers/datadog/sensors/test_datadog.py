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

import json
import unittest
from typing import List

from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.datadog.sensors.datadog import DatadogSensor
from airflow.utils import db

at_least_one_event = [
    {
        'alert_type': 'info',
        'comments': [],
        'date_happened': 1419436860,
        'device_name': None,
        'host': None,
        'id': 2603387619536318140,
        'is_aggregate': False,
        'priority': 'normal',
        'resource': '/api/v1/events/2603387619536318140',
        'source': 'My Apps',
        'tags': ['application:web', 'version:1'],
        'text': 'And let me tell you all about it here!',
        'title': 'Something big happened!',
        'url': '/event/jump_to?event_id=2603387619536318140',
    },
    {
        'alert_type': 'info',
        'comments': [],
        'date_happened': 1419436865,
        'device_name': None,
        'host': None,
        'id': 2603387619536318141,
        'is_aggregate': False,
        'priority': 'normal',
        'resource': '/api/v1/events/2603387619536318141',
        'source': 'My Apps',
        'tags': ['application:web', 'version:1'],
        'text': 'And let me tell you all about it here!',
        'title': 'Something big happened!',
        'url': '/event/jump_to?event_id=2603387619536318141',
    },
]

zero_events = []  # type: List


class TestDatadogSensor(unittest.TestCase):
    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='datadog_default',
                conn_type='datadog',
                login='login',
                password='password',
                extra=json.dumps({'api_key': 'api_key', 'app_key': 'app_key'}),
            )
        )

    @patch('airflow.providers.datadog.hooks.datadog.api.Event.query')
    @patch('airflow.providers.datadog.sensors.datadog.api.Event.query')
    def test_sensor_ok(self, api1, api2):
        api1.return_value = at_least_one_event
        api2.return_value = at_least_one_event

        sensor = DatadogSensor(
            task_id='test_datadog',
            datadog_conn_id='datadog_default',
            from_seconds_ago=3600,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
        )

        self.assertTrue(sensor.poke({}))

    @patch('airflow.providers.datadog.hooks.datadog.api.Event.query')
    @patch('airflow.providers.datadog.sensors.datadog.api.Event.query')
    def test_sensor_fail(self, api1, api2):
        api1.return_value = zero_events
        api2.return_value = zero_events

        sensor = DatadogSensor(
            task_id='test_datadog',
            datadog_conn_id='datadog_default',
            from_seconds_ago=0,
            up_to_seconds_from_now=0,
            priority=None,
            sources=None,
            tags=None,
            response_check=None,
        )

        self.assertFalse(sensor.poke({}))
