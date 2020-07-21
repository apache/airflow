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
import unittest
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.segment.hooks.segment import SegmentHook

TEST_CONN_ID = 'test_segment'
WRITE_KEY = 'foo'


class TestSegmentHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.conn = conn = mock.MagicMock()
        conn.write_key = WRITE_KEY
        self.expected_write_key = WRITE_KEY
        self.conn.extra_dejson = {'write_key': self.expected_write_key}

        class UnitTestSegmentHook(SegmentHook):

            def get_conn(self):
                return conn

            def get_connection(self, _):
                return conn

        self.test_hook = UnitTestSegmentHook(segment_conn_id=TEST_CONN_ID)

    def test_get_conn(self):
        expected_connection = self.test_hook.get_conn()
        self.assertEqual(expected_connection, self.conn)
        self.assertIsNotNone(expected_connection.write_key)
        self.assertEqual(expected_connection.write_key, self.expected_write_key)

    def test_on_error(self):
        with self.assertRaises(AirflowException):
            self.test_hook.on_error('error', ['items'])
