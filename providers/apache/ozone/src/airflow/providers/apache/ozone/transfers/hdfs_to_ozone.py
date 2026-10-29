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

import logging
import shutil
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.apache.ozone.hooks.ozone import (
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
)
from airflow.providers.apache.ozone.utils.cli_runner import CliRunner
from airflow.providers.apache.ozone.utils.connection_schema import (
    OzoneConnSnapshot,
)
from airflow.providers.apache.ozone.utils.errors import OzoneProviderError
from airflow.providers.apache.ozone.utils.security import (
    KerberosConfig,
    SSLConfig,
)
from airflow.providers.apache.ozone.version_compat import (
    AirflowException,
    BaseHook,
    BaseOperator,
)

if TYPE_CHECKING:
    from airflow.providers.apache.ozone.version_compat import Context

log = logging.getLogger(__name__)
DISTCP_BASE_COMMAND = ["hadoop", "distcp"]
DISTCP_COPY_OPTIONS = ["-update", "-skipcrccheck"]
DISTCP_MAPREDUCE_LOCAL_OPTION = "-Dmapreduce.framework.name=local"
DISTCP_YARN_RENEWER_PRINCIPAL_OPTION = "-Dyarn.resourcemanager.principal={principal}"
DISTCP_JOBTRACKER_RENEWER_PRINCIPAL_OPTION = "-Dmapreduce.jobtracker.kerberos.principal={principal}"


class HdfsToOzoneOperator(BaseOperator):
    """
    Migrate data from HDFS to Ozone using DistCp.

    Optional HDFS SSL/TLS and Kerberos settings are read from the
    ``hdfs_conn_id`` connection extra.
    """

    template_fields = ("source_path", "dest_path", "hdfs_conn_id")

    def __init__(
        self,
        source_path: str,
        dest_path: str,
        hdfs_conn_id: str | None = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if hdfs_conn_id is not None and not isinstance(hdfs_conn_id, str):
            raise ValueError(f"hdfs_conn_id must be a string or None, got {type(hdfs_conn_id).__name__}")
        self.source_path = source_path
        self.dest_path = dest_path
        self.hdfs_conn_id = hdfs_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

        self.log.debug(
            "Initializing HdfsToOzoneOperator - source: %s, destination: %s", self.source_path, self.dest_path
        )

    @cached_property
    def _hdfs_ssl_env(self) -> dict[str, str] | None:
        """Load SSL/TLS configuration from HDFS snapshot lazily."""
        if not self._hdfs_connection_snapshot:
            return None
        try:
            ssl_env_vars = SSLConfig.from_snapshot(
                self._hdfs_connection_snapshot,
                conn_id=self.hdfs_conn_id,
                scope="hdfs",
            ).as_env()
            if not ssl_env_vars:
                self.log.debug("No HDFS SSL/TLS configuration found in connection snapshot")
                return None
            ssl_env = SSLConfig.apply_ssl_env_vars(ssl_env_vars)
            self.log.debug("HDFS SSL/TLS configuration loaded from connection: %s", list(ssl_env_vars.keys()))
            if self._hdfs_connection_snapshot.hdfs_ssl_enabled:
                self.log.info("HDFS SSL/TLS enabled for connection")
            return ssl_env
        except AirflowException as err:
            # Connection might not exist yet, that's OK
            self.log.debug("Could not load HDFS SSL configuration (connection may not exist): %s", str(err))
        return None

    @cached_property
    def _hdfs_connection_snapshot(self) -> OzoneConnSnapshot | None:
        """Read HDFS connection snapshot once for Kerberos/SSL helpers."""
        if not self.hdfs_conn_id:
            return None
        try:
            conn = BaseHook.get_connection(self.hdfs_conn_id)
            return OzoneConnSnapshot.from_connection(
                conn,
                conn_id=self.hdfs_conn_id,
                require_host_port=False,
            )
        except AirflowException as err:
            self.log.debug("Could not load HDFS connection extra (connection may not exist): %s", str(err))
        return None

    @cached_property
    def _hdfs_kerberos_env(self) -> dict[str, str] | None:
        """Load HDFS Kerberos subprocess env from the connection snapshot."""
        if not self.hdfs_conn_id:
            return None
        if not self._hdfs_connection_snapshot:
            return None
        return KerberosConfig.load_hdfs_env(
            snapshot=self._hdfs_connection_snapshot,
            conn_id=self.hdfs_conn_id,
        )

    def _build_distcp_env(self) -> dict[str, str] | None:
        """Build DistCp env and initialize HDFS Kerberos when configured."""
        env: dict[str, str] = {}
        if self._hdfs_ssl_env:
            self.log.debug("Applying SSL environment variables for HDFS DistCp")
            env.update(self._hdfs_ssl_env)

        if self._hdfs_kerberos_env:
            snapshot = self._hdfs_connection_snapshot
            if snapshot is None:
                raise OzoneProviderError(
                    "HDFS Kerberos environment exists but HDFS connection snapshot is missing."
                )
            if not KerberosConfig.kinit_hdfs_from_snapshot(
                snapshot=snapshot,
                conn_id=self.hdfs_conn_id,
            ):
                raise OzoneProviderError(
                    f"HDFS Kerberos authentication failed for connection '{self.hdfs_conn_id}' "
                    f"using principal '{snapshot.hdfs_kerberos_principal}'."
                )
            env.update(self._hdfs_kerberos_env)
            env["HADOOP_SECURITY_AUTHENTICATION"] = "kerberos"
            if snapshot.krb5_conf:
                env["KRB5_CONFIG"] = snapshot.krb5_conf

        return env or None

    def _build_distcp_command(self) -> list[str]:
        """Build the DistCp command for the current transfer."""
        options: list[str] = []
        snapshot = self._hdfs_connection_snapshot
        if snapshot:
            if snapshot.hdfs_distcp_mapreduce_local:
                options.append(DISTCP_MAPREDUCE_LOCAL_OPTION)
            if snapshot.hdfs_distcp_renewer_principal:
                principal = snapshot.hdfs_distcp_renewer_principal
                options.extend(
                    [
                        DISTCP_YARN_RENEWER_PRINCIPAL_OPTION.format(principal=principal),
                        DISTCP_JOBTRACKER_RENEWER_PRINCIPAL_OPTION.format(principal=principal),
                    ]
                )
        return [*DISTCP_BASE_COMMAND, *options, *DISTCP_COPY_OPTIONS, self.source_path, self.dest_path]

    def _validate_runtime_inputs(self) -> None:
        """Validate operator inputs right before DistCp execution."""
        if not isinstance(self.source_path, str):
            raise OzoneProviderError("HdfsToOzoneOperator requires source_path to be a string")
        if not self.source_path.strip():
            raise OzoneProviderError("HdfsToOzoneOperator requires non-empty source_path")

        if not isinstance(self.dest_path, str):
            raise OzoneProviderError("HdfsToOzoneOperator requires dest_path to be a string")
        if not self.dest_path.strip():
            raise OzoneProviderError("HdfsToOzoneOperator requires non-empty dest_path")

    def _validate_hadoop_runtime(self) -> str:
        """Check that the Hadoop DistCp runtime binary is available."""
        hadoop_bin = shutil.which("hadoop")
        if not hadoop_bin:
            raise OzoneProviderError(
                "HdfsToOzoneOperator is loaded, but Hadoop DistCp runtime is unavailable: "
                "executable 'hadoop' was not found in PATH. "
                "Install Hadoop client tools or provide 'hadoop' in PATH on the worker that runs this task"
            )
        return hadoop_bin

    def _validate_runtime_dependencies(self) -> None:
        """Run fail-first runtime validation for DistCp prerequisites."""
        self._validate_runtime_inputs()
        self._validate_hadoop_runtime()

    def execute(self, context: Context) -> None:
        self.log.info("Starting DistCp migration: %s -> %s", self.source_path, self.dest_path)

        self._validate_runtime_dependencies()
        cmd = self._build_distcp_command()
        distcp_env = self._build_distcp_env()

        CliRunner.run_process(
            cmd,
            env_overrides=distcp_env,
            timeout=self.timeout,
            retry_attempts=self.retry_attempts,
            check=True,
            log_output=True,
        )
        self.log.info("DistCp migration completed")
