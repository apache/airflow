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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.apache.spark.hooks.spark_pipelines import SparkPipelinesHook
from airflow.providers.common.compat.sdk import AirflowException


class TestSparkPipelinesHook:
    def setup_method(self):
        self.hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml", pipeline_command="run", conn_id="spark_default"
        )

    def test_init_with_valid_command(self):
        hook = SparkPipelinesHook(pipeline_command="run")
        assert hook.pipeline_command == "run"

    def test_init_with_invalid_command(self):
        with pytest.raises(ValueError, match="Invalid pipeline command"):
            SparkPipelinesHook(pipeline_command="invalid")

    def test_get_spark_binary_path(self):
        binary_path = self.hook._get_spark_binary_path()
        assert binary_path == ["spark-pipelines"]

    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook._resolve_connection")
    def test_build_pipelines_command_run(self, mock_resolve_connection):
        mock_resolve_connection.return_value = {
            "master": "yarn",
            "deploy_mode": "client",
            "queue": "default",
            "keytab": None,
            "principal": None,
        }

        hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            num_executors=2,
            executor_cores=4,
            executor_memory="2G",
            driver_memory="1G",
            verbose=True,
        )
        hook._connection = mock_resolve_connection.return_value

        cmd = hook._build_spark_pipelines_command()

        # Verify the command starts correctly and contains expected arguments
        assert cmd[0] == "spark-pipelines"
        assert cmd[1] == "run"
        assert "--spec" in cmd
        assert "test_pipeline.yml" in cmd
        assert "--master" in cmd
        assert "yarn" in cmd
        assert "--deploy-mode" in cmd
        assert "client" in cmd
        assert "--queue" in cmd
        assert "default" in cmd
        assert "--num-executors" in cmd
        assert "2" in cmd
        assert "--executor-cores" in cmd
        assert "4" in cmd
        assert "--executor-memory" in cmd
        assert "2G" in cmd
        assert "--driver-memory" in cmd
        assert "1G" in cmd
        assert "--verbose" in cmd

    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook._resolve_connection")
    def test_build_pipelines_command_dry_run(self, mock_resolve_connection):
        mock_resolve_connection.return_value = {
            "master": "local[*]",
            "deploy_mode": None,
            "queue": None,
            "keytab": None,
            "principal": None,
        }

        hook = SparkPipelinesHook(pipeline_spec="test_pipeline.yml", pipeline_command="dry-run")
        hook._connection = mock_resolve_connection.return_value

        cmd = hook._build_spark_pipelines_command()

        # Verify the command starts correctly and contains expected arguments
        assert cmd[0] == "spark-pipelines"
        assert cmd[1] == "dry-run"
        assert "--spec" in cmd
        assert "test_pipeline.yml" in cmd
        assert "--master" in cmd
        assert "local[*]" in cmd

    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook._resolve_connection")
    def test_build_pipelines_command_with_conf(self, mock_resolve_connection):
        mock_resolve_connection.return_value = {
            "master": "yarn",
            "deploy_mode": None,
            "queue": None,
            "keytab": None,
            "principal": None,
        }

        hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            conf={
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
            },
        )
        hook._connection = mock_resolve_connection.return_value

        cmd = hook._build_spark_pipelines_command()

        assert "--conf" in cmd
        assert "spark.sql.adaptive.enabled=true" in cmd
        assert "spark.sql.adaptive.coalescePartitions.enabled=true" in cmd

    @patch("subprocess.Popen")
    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook._resolve_connection")
    def test_submit_pipeline_success(self, mock_resolve_connection, mock_popen):
        mock_resolve_connection.return_value = {
            "master": "yarn",
            "deploy_mode": None,
            "queue": None,
            "keytab": None,
            "principal": None,
        }

        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_process.stdout = ["Pipeline completed successfully"]
        mock_popen.return_value = mock_process

        self.hook._connection = mock_resolve_connection.return_value
        self.hook.submit_pipeline()

        mock_popen.assert_called_once()
        mock_process.wait.assert_called_once()

    @patch("subprocess.Popen")
    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook._resolve_connection")
    def test_submit_pipeline_failure(self, mock_resolve_connection, mock_popen):
        mock_resolve_connection.return_value = {
            "master": "yarn",
            "deploy_mode": None,
            "queue": None,
            "keytab": None,
            "principal": None,
        }

        mock_process = MagicMock()
        mock_process.wait.return_value = 1
        mock_process.stdout = ["Pipeline failed"]
        mock_popen.return_value = mock_process

        self.hook._connection = mock_resolve_connection.return_value

        with pytest.raises(AirflowException, match="Cannot execute"):
            self.hook.submit_pipeline()

    def test_submit_calls_submit_pipeline(self):
        with patch.object(self.hook, "submit_pipeline") as mock_submit_pipeline:
            self.hook.submit("dummy_application")
            mock_submit_pipeline.assert_called_once()
