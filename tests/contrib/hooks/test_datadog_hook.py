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
import json
import mock
import unittest

from airflow.exceptions import AirflowException
from airflow.models import Connection

from airflow.contrib.hooks.datadog_hook import DatadogHook


APP_KEY = 'app_key'
API_KEY = 'api_key'
METRIC_NAME = 'metric'
DATAPOINT = 7
TAGS = ['tag']
TYPE = 'rate'
INTERVAL = 30
TITLE = 'title'
TEXT = 'text'
AGGREGATION_KEY = 'aggregation-key'
ALERT_TYPE = 'warning'
DATE_HAPPENED = 12345
HANDLE = 'handle'
PRIORITY = 'normal'
RELATED_EVENT_ID = 7
DEVICE_NAME = 'device-name'


class TestDatadogHook(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.datadog_hook.initialize')
    @mock.patch('airflow.contrib.hooks.datadog_hook.DatadogHook.get_connection')
    def setUp(self, mock_get_connection, mock_initialize):
        mock_get_connection.return_value = Connection(extra=json.dumps({
            'app_key': APP_KEY,
            'api_key': API_KEY,
        }))
        self.hook = DatadogHook()

    @mock.patch('airflow.contrib.hooks.datadog_hook.initialize')
    @mock.patch('airflow.contrib.hooks.datadog_hook.DatadogHook.get_connection')
    def test_api_key_required(self, mock_get_connection, mock_initialize):
        mock_get_connection.return_value = Connection()
        with self.assertRaises(AirflowException) as ctx:
            DatadogHook()
        self.assertEqual(str(ctx.exception),
                         'api_key must be specified in the Datadog connection details')

    def test_validate_response_valid(self):
        try:
            self.hook.validate_response({'status': 'ok'})
        except AirflowException:
            self.fail('Unexpected AirflowException raised')

    def test_validate_response_invalid(self):
        with self.assertRaises(AirflowException):
            self.hook.validate_response({'status': 'error'})

    @mock.patch('airflow.contrib.hooks.datadog_hook.api.Metric.send')
    def test_send_metric(self, mock_send):
        mock_send.return_value = {'status': 'ok'}
        self.hook.send_metric(
            METRIC_NAME,
            DATAPOINT,
            tags=TAGS,
            type_=TYPE,
            interval=INTERVAL,
        )
        mock_send.assert_called_with(
            metric=METRIC_NAME,
            points=DATAPOINT,
            host=self.hook.host,
            tags=TAGS,
            type=TYPE,
            interval=INTERVAL,
        )

    @mock.patch('airflow.contrib.hooks.datadog_hook.api.Metric.query')
    @mock.patch('airflow.contrib.hooks.datadog_hook.time.time')
    def test_query_metric(self, mock_time, mock_query):
        now = 12345
        mock_time.return_value = now
        mock_query.return_value = {'status': 'ok'}
        self.hook.query_metric('query', 60, 30)
        mock_query.assert_called_with(
            start=now - 60,
            end=now - 30,
            query='query',
        )

    @mock.patch('airflow.contrib.hooks.datadog_hook.api.Event.create')
    def test_post_event(self, mock_create):
        mock_create.return_value = {'status': 'ok'}
        self.hook.post_event(
            TITLE,
            TEXT,
            aggregation_key=AGGREGATION_KEY,
            alert_type=ALERT_TYPE,
            date_happened=DATE_HAPPENED,
            handle=HANDLE,
            priority=PRIORITY,
            related_event_id=RELATED_EVENT_ID,
            tags=TAGS,
            device_name=DEVICE_NAME,
        )
        mock_create.assert_called_with(
            title=TITLE,
            text=TEXT,
            aggregation_key=AGGREGATION_KEY,
            alert_type=ALERT_TYPE,
            date_happened=DATE_HAPPENED,
            handle=HANDLE,
            priority=PRIORITY,
            related_event_id=RELATED_EVENT_ID,
            tags=TAGS,
            host=self.hook.host,
            device_name=DEVICE_NAME,
            source_type_name=self.hook.source_type_name,
        )


if __name__ == '__main__':
    unittest.main()
