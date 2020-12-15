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

from airflow import configuration
from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from airflow.providers.amazon.aws.sensors.glue_crawler import AwsGlueCrawlerSensor


class TestAwsGlueCrawlerSensor(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()

    @mock.patch.object(AwsGlueCrawlerHook, 'get_conn')
    @mock.patch.object(AwsGlueCrawlerHook, 'get_crawler_state')
    def test_poke(self, mock_get_crawler_status, mock_conn):
        mock_conn.return_value.get_crawler_state()
        mock_get_crawler_state.return_value = 'SUCCEEDED'
        op = AwsGlueCrawlerSensor(
            task_id='test_glue_crawler_sensor',
            crawler_name='aws_test_glue_crawler',
            poke_interval=1,
            timeout=5,
            aws_conn_id='aws_default',
        )
        self.assertTrue(op.poke(None))

    @mock.patch.object(AwsGlueCrawlerHook, 'get_conn')
    @mock.patch.object(AwsGlueCrawlerHook, 'get_crawler_state')
    def test_poke_false(self, mock_get_crawler_state, mock_conn):
        mock_conn.return_value.get_crawler_run()
        mock_get_crawler_state.return_value = 'RUNNING'
        op = AwsGlueCrawlerSensor(
            task_id='test_glue_crawler_sensor',
            crawler_name='aws_test_glue_Crawler',
            poke_interval=1,
            timeout=5,
            aws_conn_id='aws_default',
        )
        self.assertFalse(op.poke(None))


if __name__ == '__main__':
    unittest.main()
