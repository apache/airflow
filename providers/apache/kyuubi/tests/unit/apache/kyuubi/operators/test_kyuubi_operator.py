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

from airflow.providers.apache.kyuubi.hooks.kyuubi import KyuubiHook
from airflow.providers.apache.kyuubi.operators.kyuubi import KyuubiOperator


class TestKyuubiOperator(unittest.TestCase):
    def test_init(self):
        op = KyuubiOperator(task_id="test", hql="SELECT 1")
        assert op.kyuubi_conn_id == "kyuubi_default"
        # Check that it passes kyuubi_conn_id as hive_cli_conn_id to base class
        assert op.hive_cli_conn_id == "kyuubi_default"

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.get_connection")
    def test_hook_property(self, mock_get_connection):
        conn = mock.MagicMock()
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        op = KyuubiOperator(
            task_id="test",
            hql="SELECT 1",
            kyuubi_conn_id="my_kyuubi_conn",
            spark_queue="test_queue",
            spark_app_name="test_app",
            spark_conf={"key": "value"},
        )
        hook = op.hook
        assert isinstance(hook, KyuubiHook)
        mock_get_connection.assert_called_with("my_kyuubi_conn")
        assert hook.mapred_queue == "test_queue"
        assert hook.mapred_job_name == "test_app"
        assert hook.spark_conf == {"key": "value"}
