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
from __future__ import annotations

import unittest
from unittest import mock
from unittest.mock import PropertyMock

from airflow.providers.alibaba.cloud.sensors.oss_key import OSSKeySensor

OSS_SENSOR_STRING = "airflow.providers.alibaba.cloud.sensors.oss_key.{}"
MOCK_TASK_ID = "test-oss-operator"
MOCK_REGION = "mock_region"
MOCK_BUCKET = "mock_bucket_name"
MOCK_OSS_CONN_ID = "mock_oss_conn_default"
MOCK_KEY = "mock_key"
MOCK_KEYS = ["mock_key1", "mock_key_2", "mock_key3"]
MOCK_CONTENT = "mock_content"


class TestOSSKeySensor(unittest.TestCase):
    def setUp(self):
        self.sensor = OSSKeySensor(
            bucket_key=MOCK_KEY,
            oss_conn_id=MOCK_OSS_CONN_ID,
            region=MOCK_REGION,
            bucket_name=MOCK_BUCKET,
            task_id=MOCK_TASK_ID,
        )

    @mock.patch(OSS_SENSOR_STRING.format("OSSHook"))
    def test_get_hook(self, mock_service):
        self.sensor.get_hook()
        mock_service.assert_called_once_with(oss_conn_id=MOCK_OSS_CONN_ID, region=MOCK_REGION)

    @mock.patch(OSS_SENSOR_STRING.format("OSSKeySensor.get_hook"), new_callable=PropertyMock)
    def test_poke_exsiting_key(self, mock_service):
        # Given
        mock_service.return_value.object_exists.return_value = True

        # When
        res = self.sensor.poke(None)

        # Then
        assert res is True
        mock_service.return_value.object_exists.assert_called_once_with(key=MOCK_KEY, bucket_name=MOCK_BUCKET)

    @mock.patch(OSS_SENSOR_STRING.format("OSSKeySensor.get_hook"), new_callable=PropertyMock)
    def test_poke_non_exsiting_key(self, mock_service):
        # Given
        mock_service.return_value.object_exists.return_value = False

        # When
        res = self.sensor.poke(None)

        # Then
        assert res is False
        mock_service.return_value.object_exists.assert_called_once_with(key=MOCK_KEY, bucket_name=MOCK_BUCKET)
