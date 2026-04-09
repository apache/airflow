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

import subprocess
from typing import Any

from airflow.providers.apache.spark.hooks.spark_submit import SparkSubmitHook
from airflow.providers.common.compat.sdk import AirflowException


class SparkPipelinesException(AirflowException):
    """Exception raised when spark-pipelines command fails."""


class SparkPipelinesHook(SparkSubmitHook):
    """
    Hook for interacting with Spark Declarative Pipelines via the spark-pipelines CLI.

    Extends SparkSubmitHook to leverage existing connection management while providing
    pipeline-specific functionality.

    :param pipeline_spec: Path to the pipeline specification file (YAML)
    :param pipeline_command: The spark-pipelines command to run ('run', 'dry-run')
    """

    def __init__(
        self,
        pipeline_spec: str | None = None,
        pipeline_command: str = "run",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.pipeline_spec = pipeline_spec
        self.pipeline_command = pipeline_command

        if pipeline_command not in ["run", "dry-run"]:
            raise ValueError(f"Invalid pipeline command: {pipeline_command}. Must be 'run' or 'dry-run'")

    def _get_spark_binary_path(self) -> list[str]:
        return ["spark-pipelines"]

    def _build_spark_pipelines_command(self) -> list[str]:
        """
        Construct the spark-pipelines command to execute.

        :return: full command to be executed
        """
        # Start with spark-pipelines binary and command
        connection_cmd = self._get_spark_binary_path()
        connection_cmd.append(self.pipeline_command)

        # Add pipeline spec if provided
        if self.pipeline_spec:
            connection_cmd.extend(["--spec", self.pipeline_spec])

        # Reuse parent's common spark argument building logic
        connection_cmd.extend(self._build_spark_common_args())

        self.log.info("Spark-Pipelines cmd: %s", self._mask_cmd(connection_cmd))
        return connection_cmd

    def submit_pipeline(self, **kwargs: Any) -> None:
        """
        Execute the spark-pipelines command.

        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        pipelines_cmd = self._build_spark_pipelines_command()

        if self._env:
            import os

            env = os.environ.copy()
            env.update(self._env)
            kwargs["env"] = env

        self._submit_sp = subprocess.Popen(
            pipelines_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=-1,
            universal_newlines=True,
            **kwargs,
        )

        if self._submit_sp.stdout:
            self._process_spark_submit_log(iter(self._submit_sp.stdout))
        returncode = self._submit_sp.wait()

        if returncode:
            raise SparkPipelinesException(
                f"Cannot execute: {self._mask_cmd(pipelines_cmd)}. Error code is: {returncode}."
            )

    def submit(self, application: str = "", **kwargs: Any) -> None:
        """Override submit to use pipeline-specific logic."""
        self.submit_pipeline(**kwargs)
