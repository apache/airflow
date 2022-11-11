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
from __future__ import annotations

import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor


class TestGlueCrawlerSensor(unittest.TestCase):
    def setUp(self):
        self.sensor = GlueCrawlerSensor(
            task_id="test_glue_crawler_sensor",
            crawler_name="aws_test_glue_crawler",
            poke_interval=1,
            timeout=5,
            aws_conn_id="aws_default",
        )

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    def test_poke_success(self, mock_get_crawler):
        mock_get_crawler.return_value["LastCrawl"]["Status"] = "SUCCEEDED"
        self.assertFalse(self.sensor.poke({}))
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    def test_poke_failed(self, mock_get_crawler):
        mock_get_crawler.return_value["LastCrawl"]["Status"] = "FAILED"
        self.assertFalse(self.sensor.poke({}))
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    def test_poke_cancelled(self, mock_get_crawler):
        mock_get_crawler.return_value["LastCrawl"]["Status"] = "CANCELLED"
        self.assertFalse(self.sensor.poke({}))
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")


if __name__ == "__main__":
    unittest.main()
