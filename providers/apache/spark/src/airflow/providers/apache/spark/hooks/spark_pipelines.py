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

import os
import subprocess
import sys
from typing import Any

from airflow.providers.apache.spark.hooks.spark_submit import DEFAULT_SPARK_BINARY, SparkSubmitHook
from airflow.providers.common.compat.sdk import AirflowException, AirflowNotFoundException


class SparkPipelinesException(AirflowException):
    """Exception raised when spark-pipelines command fails."""


class SparkPipelinesHook(SparkSubmitHook):
    """
    Hook for interacting with Spark Declarative Pipelines via the spark-pipelines CLI.

    Extends SparkSubmitHook to leverage existing connection management while providing
    pipeline-specific functionality.

    Two connection modes are supported:

    * Legacy spark-submit-style (``spark`` / ``yarn`` / ``k8s`` connection types) —
      invokes the ``spark-pipelines`` launcher with ``--master``, ``--deploy-mode``
      and the rest of the standard cluster-manager flags assembled by
      :class:`~airflow.providers.apache.spark.hooks.spark_submit.SparkSubmitHook`.
    * Spark Connect (``spark_connect`` connection type, Spark 4.x+) — sets
      ``SPARK_REMOTE`` from the connection's ``sc://`` URI and invokes the
      Connect-native ``pyspark.pipelines.cli`` Python module directly. The
      cluster-manager flags are *not* emitted: the Connect-native CLI rejects
      them with ``SparkException: Remote cannot be specified with master and/or
      deploy mode``, and the ``spark-pipelines`` bash launcher itself starts a
      JVM ``SparkContext`` that collides with the Connect daemon's gRPC port.

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

    def _resolve_connection(self) -> dict[str, Any]:
        """
        Resolve the configured connection, branching on Spark Connect.

        For ``spark_connect``-typed connections, populate a ``spark_remote`` key
        with the ``sc://`` URI rendered by
        :class:`~airflow.providers.apache.spark.hooks.spark_connect.SparkConnectHook`
        and zero out the spark-submit cluster-manager fields — the Connect-native
        CLI doesn't consume them. For every other connection type, defer to the
        parent's resolver so spark-submit-style behaviour is unchanged.
        """
        try:
            conn = self.get_connection(self._conn_id)
        except AirflowNotFoundException:
            # No connection configured — fall through to spark-submit defaults.
            return super()._resolve_connection()

        if conn.conn_type != "spark_connect":
            return super()._resolve_connection()

        # Local import: SparkConnectHook lives in the same provider but loading
        # it eagerly would create an unnecessary cycle for spark-submit-only
        # deployments that never touch Connect.
        from airflow.providers.apache.spark.hooks.spark_connect import SparkConnectHook

        return {
            # ``master`` is consumed by the parent's ``__init__`` (substring
            # checks for ``yarn``/``k8s``/``spark://``); leave it empty so none
            # of those branches match. Connect mode never calls
            # ``_build_spark_common_args``, which is the only thing that would
            # actually emit ``--master`` to the CLI.
            "master": "",
            "queue": None,
            "deploy_mode": None,
            "spark_binary": self.spark_binary or DEFAULT_SPARK_BINARY,
            "namespace": None,
            "principal": self._principal,
            "keytab": self._keytab,
            "spark_remote": SparkConnectHook(conn_id=self._conn_id).get_connection_url(),
        }

    def _get_spark_binary_path(self) -> list[str]:
        if self._connection.get("spark_remote"):
            # The ``spark-pipelines`` bash launcher routes through
            # ``spark-class`` → ``SparkSubmit`` → JVM ``SparkContext``, which
            # appends cluster-manager flags the Connect-native CLI rejects and
            # binds an in-process Connect server that collides with the
            # long-running daemon. Invoke the underlying Python module directly
            # so ``SparkSession.builder.getOrCreate()`` becomes a Connect client.
            return [sys.executable, "-m", "pyspark.pipelines.cli"]
        return ["spark-pipelines"]

    def _build_spark_pipelines_command(self) -> list[str]:
        """
        Construct the spark-pipelines command to execute.

        :return: full command to be executed
        """
        connection_cmd = self._get_spark_binary_path()
        connection_cmd.append(self.pipeline_command)

        if self.pipeline_spec:
            connection_cmd.extend(["--spec", self.pipeline_spec])

        if not self._connection.get("spark_remote"):
            # Reuse parent's common spark argument building logic. The
            # Connect-native CLI rejects --master/--deploy-mode/--name, so only
            # emit them for legacy spark-submit-style connections.
            connection_cmd.extend(self._build_spark_common_args())

        self.log.info("Spark-Pipelines cmd: %s", self._mask_cmd(connection_cmd))
        return connection_cmd

    def submit_pipeline(self, **kwargs: Any) -> None:
        """
        Execute the spark-pipelines command.

        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        pipelines_cmd = self._build_spark_pipelines_command()
        spark_remote = self._connection.get("spark_remote")

        # ``self._env`` is only populated by ``_build_spark_common_args`` —
        # which the Connect path skips — so fall back to ``self._env_vars``
        # (the operator-supplied env_vars kwarg) for that path.
        env_overrides: dict[str, str] = dict(self._env or self._env_vars or {})
        if spark_remote:
            # Don't clobber a SPARK_REMOTE the operator caller already set via
            # env_vars; that takes precedence for failover routing.
            env_overrides.setdefault("SPARK_REMOTE", spark_remote)

        if env_overrides:
            env = os.environ.copy()
            env.update(env_overrides)
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
