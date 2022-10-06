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

from unittest import mock

from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from tests.providers.apache.hive import TestHiveEnvironment

TEST_HDFS_CONN = 'webhdfs_default'
TEST_HDFS_PATH = 'hdfs://user/hive/warehouse/airflow.db/static_babynames'


class TestWebHdfsSensor(TestHiveEnvironment):
    @mock.patch('airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook')
    def test_poke(self, mock_hook):
        sensor = WebHdfsSensor(
            task_id='test_task',
            webhdfs_conn_id=TEST_HDFS_CONN,
            filepath=TEST_HDFS_PATH,
        )
        exists = sensor.poke(dict())

        assert exists

        mock_hook.return_value.check_for_path.assert_called_once_with(hdfs_path=TEST_HDFS_PATH)
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)

    @mock.patch('airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook')
    def test_poke_should_return_false_for_non_existing_table(self, mock_hook):
        mock_hook.return_value.check_for_path.return_value = False

        sensor = WebHdfsSensor(
            task_id='test_task',
            webhdfs_conn_id=TEST_HDFS_CONN,
            filepath=TEST_HDFS_PATH,
        )
        exists = sensor.poke(dict())

        assert not exists

        mock_hook.return_value.check_for_path.assert_called_once_with(hdfs_path=TEST_HDFS_PATH)
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)
