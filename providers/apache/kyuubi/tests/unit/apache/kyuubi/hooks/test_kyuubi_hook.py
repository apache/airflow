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


class TestKyuubiHook(unittest.TestCase):
    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.get_connection")
    def test_init(self, mock_get_connection):
        conn = mock.MagicMock()
        conn.extra_dejson = {}
        mock_get_connection.return_value = conn

        hook = KyuubiHook(
            spark_queue="test_queue",
            spark_app_name="test_app",
            spark_sql_shuffle_partitions="200",
            spark_conf={"spark.executor.memory": "4g"},
        )
        assert hook.conn_name_attr == "kyuubi_conn_id"
        assert hook.default_conn_name == "kyuubi_default"
        assert hook.conn_type == "kyuubi"
        assert hook.mapred_queue == "test_queue"
        assert hook.mapred_job_name == "test_app"
        assert hook.spark_sql_shuffle_partitions == "200"
        assert hook.spark_conf == {"spark.executor.memory": "4g"}

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.get_connection")
    def test_prepare_cli_cmd_with_custom_beeline(self, mock_get_connection):
        # Mock connection with extra beeline_path
        conn = mock.MagicMock()
        conn.extra_dejson = {"beeline_path": "/custom/path/to/beeline", "use_beeline": True}
        conn.host = "localhost"
        conn.port = 10000
        conn.schema = "default"
        conn.login = "user"
        conn.password = "password"
        mock_get_connection.return_value = conn

        hook = KyuubiHook()
        # Ensure use_beeline is True as it comes from conn.extra_dejson in __init__
        # But we mocked get_connection, so __init__ called it.
        # Check if hook.use_beeline is set correctly
        assert hook.use_beeline is True
        
        cmd = hook._prepare_cli_cmd()
        assert cmd[0] == "/custom/path/to/beeline"

    @mock.patch("airflow.providers.apache.hive.hooks.hive.HiveCliHook.get_connection")
    def test_prepare_cli_cmd_default(self, mock_get_connection):
        # Mock connection without beeline_path
        conn = mock.MagicMock()
        conn.extra_dejson = {"use_beeline": True}
        conn.host = "localhost"
        conn.port = 10000
        conn.schema = "default"
        mock_get_connection.return_value = conn

        hook = KyuubiHook()
        
        cmd = hook._prepare_cli_cmd()
        assert cmd[0] == "beeline"

    def test_get_connection_form_widgets(self):
        widgets = KyuubiHook.get_connection_form_widgets()
        assert "beeline_path" in widgets
        assert "use_beeline" in widgets
