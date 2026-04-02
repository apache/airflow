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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook
from airflow.providers.amazon.aws.sensors.glue_crawler import GlueCrawlerSensor
from airflow.providers.common.compat.sdk import AirflowException


class TestGlueCrawlerSensor:
    def setup_method(self):
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
        assert self.sensor.poke({}) is False
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    def test_poke_failed(self, mock_get_crawler):
        mock_get_crawler.return_value["LastCrawl"]["Status"] = "FAILED"
        assert self.sensor.poke({}) is False
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    def test_poke_cancelled(self, mock_get_crawler):
        mock_get_crawler.return_value["LastCrawl"]["Status"] = "CANCELLED"
        assert self.sensor.poke({}) is False
        mock_get_crawler.assert_called_once_with("aws_test_glue_crawler")

    @mock.patch("airflow.providers.amazon.aws.hooks.glue_crawler.GlueCrawlerHook.get_crawler")
    def test_fail_poke(self, get_crawler):
        crawler_status = "FAILED"
        get_crawler.return_value = {"State": "READY", "LastCrawl": {"Status": crawler_status}}
        message = f"Status: {crawler_status}"
        with pytest.raises(AirflowException, match=message):
            self.sensor.poke(context={})

    def test_base_aws_op_attributes(self):
        op = GlueCrawlerSensor(
            task_id="test_glue_crawler_sensor",
            crawler_name="aws_test_glue_crawler",
        )
        assert op.hook.client_type == "glue"
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = GlueCrawlerSensor(
            task_id="test_glue_crawler_sensor",
            crawler_name="aws_test_glue_crawler",
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42
