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

import sys
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

    # ------------------------------------------------------------------ #
    # Spark Connect path: spark_connect-typed conn_id should bypass the   #
    # spark-submit launcher and the cluster-manager flags entirely, and   #
    # set SPARK_REMOTE in the subprocess environment instead.             #
    # ------------------------------------------------------------------ #

    @patch(
        "airflow.providers.apache.spark.hooks.spark_connect.SparkConnectHook.get_connection_url",
        return_value="sc://spark-connect.example:15002/",
    )
    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook.get_connection")
    def test_build_pipelines_command_spark_connect_skips_cluster_args(
        self, mock_get_connection, mock_get_url
    ):
        spark_connect_conn = MagicMock()
        spark_connect_conn.conn_type = "spark_connect"
        spark_connect_conn.host = "spark-connect.example"
        spark_connect_conn.port = 15002
        spark_connect_conn.login = None
        spark_connect_conn.password = None
        # SparkSubmitHook.__init__ -> _resolve_connection() reads
        # extra_dejson.get("queue"|"deploy-mode"|"spark-binary"|"namespace");
        # an empty dict makes it default-and-skip cleanly.
        spark_connect_conn.extra_dejson = {}
        mock_get_connection.return_value = spark_connect_conn

        hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            conn_id="spark_connect_default",
            num_executors=2,
            executor_memory="2G",
            deploy_mode="client",
        )

        cmd = hook._build_spark_pipelines_command()

        # Connect-native CLI is invoked via the python module — bypassing the
        # bash launcher that would otherwise start a colliding JVM Connect server.
        assert cmd[:3] == [sys.executable, "-m", "pyspark.pipelines.cli"]
        assert "run" in cmd
        assert "--spec" in cmd
        assert "test_pipeline.yml" in cmd
        # Cluster-manager args MUST NOT appear: the Connect-native CLI rejects
        # them with `Remote cannot be specified with master and/or deploy mode`.
        assert "--master" not in cmd
        assert "--deploy-mode" not in cmd
        assert "--name" not in cmd
        assert "--num-executors" not in cmd
        assert "--executor-memory" not in cmd

    @patch("subprocess.Popen")
    @patch(
        "airflow.providers.apache.spark.hooks.spark_connect.SparkConnectHook.get_connection_url",
        return_value="sc://spark-connect.example:15002/",
    )
    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook.get_connection")
    def test_submit_pipeline_spark_connect_sets_spark_remote(
        self, mock_get_connection, mock_get_url, mock_popen
    ):
        spark_connect_conn = MagicMock()
        spark_connect_conn.conn_type = "spark_connect"
        spark_connect_conn.host = "spark-connect.example"
        spark_connect_conn.port = 15002
        spark_connect_conn.login = None
        spark_connect_conn.password = None
        # SparkSubmitHook.__init__ -> _resolve_connection() reads
        # extra_dejson.get("queue"|"deploy-mode"|"spark-binary"|"namespace");
        # an empty dict makes it default-and-skip cleanly.
        spark_connect_conn.extra_dejson = {}
        mock_get_connection.return_value = spark_connect_conn

        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_process.stdout = ["Run is COMPLETED."]
        mock_popen.return_value = mock_process

        hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            conn_id="spark_connect_default",
        )
        hook.submit_pipeline()

        mock_popen.assert_called_once()
        _, popen_kwargs = mock_popen.call_args
        env = popen_kwargs["env"]
        assert env["SPARK_REMOTE"] == "sc://spark-connect.example:15002/"

    @patch("subprocess.Popen")
    @patch(
        "airflow.providers.apache.spark.hooks.spark_connect.SparkConnectHook.get_connection_url",
        return_value="sc://spark-connect.example:15002/",
    )
    @patch("airflow.providers.apache.spark.hooks.spark_pipelines.SparkPipelinesHook.get_connection")
    def test_submit_pipeline_spark_connect_preserves_caller_spark_remote(
        self, mock_get_connection, mock_get_url, mock_popen
    ):
        # If the operator caller has already set SPARK_REMOTE via env_vars
        # (e.g. routing to a different daemon for failover), the hook must
        # not clobber it with the connection's URI.
        spark_connect_conn = MagicMock()
        spark_connect_conn.conn_type = "spark_connect"
        spark_connect_conn.host = "spark-connect.example"
        spark_connect_conn.port = 15002
        spark_connect_conn.login = None
        spark_connect_conn.password = None
        # SparkSubmitHook.__init__ -> _resolve_connection() reads
        # extra_dejson.get("queue"|"deploy-mode"|"spark-binary"|"namespace");
        # an empty dict makes it default-and-skip cleanly.
        spark_connect_conn.extra_dejson = {}
        mock_get_connection.return_value = spark_connect_conn

        mock_process = MagicMock()
        mock_process.wait.return_value = 0
        mock_process.stdout = []
        mock_popen.return_value = mock_process

        hook = SparkPipelinesHook(
            pipeline_spec="test_pipeline.yml",
            pipeline_command="run",
            conn_id="spark_connect_default",
            env_vars={"SPARK_REMOTE": "sc://override.example:15002/"},
        )
        hook.submit_pipeline()

        _, popen_kwargs = mock_popen.call_args
        assert popen_kwargs["env"]["SPARK_REMOTE"] == "sc://override.example:15002/"
