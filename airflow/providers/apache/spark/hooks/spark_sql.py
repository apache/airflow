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
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class SparkSqlHook(BaseHook):
    """
    This hook is a wrapper around the spark-sql binary. It requires that the
    "spark-sql" binary is in the PATH.

    :param sql: The SQL query to execute
    :param conf: arbitrary Spark configuration property
    :param conn_id: connection_id string
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
        (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per
        executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab
    :param master: spark://host:port, mesos://host:port, yarn, or local
        (Default: The ``host`` and ``port`` set in the Connection, or ``"yarn"``)
    :param name: Name of the job.
    :param num_executors: Number of executors to launch
    :param verbose: Whether to pass the verbose flag to spark-sql
    :param yarn_queue: The YARN queue to submit to
        (Default: The ``queue`` value set in the Connection, or ``"default"``)
    """

    conn_name_attr = "conn_id"
    default_conn_name = "spark_sql_default"
    conn_type = "spark_sql"
    hook_name = "Spark SQL"

    def __init__(
        self,
        sql: str,
        conf: str | None = None,
        conn_id: str = default_conn_name,
        total_executor_cores: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        keytab: str | None = None,
        principal: str | None = None,
        master: str | None = None,
        name: str = "default-name",
        num_executors: int | None = None,
        verbose: bool = True,
        yarn_queue: str | None = None,
    ) -> None:
        super().__init__()
        options: dict = {}
        conn: Connection | None = None

        try:
            conn = self.get_connection(conn_id)
        except AirflowNotFoundException:
            conn = None
        if conn:
            options = conn.extra_dejson

        # Set arguments to values set in Connection if not explicitly provided.
        if master is None:
            if conn is None:
                master = "yarn"
            elif conn.port:
                master = f"{conn.host}:{conn.port}"
            else:
                master = conn.host
        if yarn_queue is None:
            yarn_queue = options.get("queue", "default")

        self._sql = sql
        self._conf = conf
        self._total_executor_cores = total_executor_cores
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._keytab = keytab
        self._principal = principal
        self._master = master
        self._name = name
        self._num_executors = num_executors
        self._verbose = verbose
        self._yarn_queue = yarn_queue
        self._sp: Any = None

    def get_conn(self) -> Any:
        pass

    def _prepare_command(self, cmd: str | list[str]) -> list[str]:
        """
        Construct the spark-sql command to execute. Verbose output is enabled
        as default.

        :param cmd: command to append to the spark-sql command
        :return: full command to be executed
        """
        connection_cmd = ["spark-sql"]
        if self._conf:
            for conf_el in self._conf.split(","):
                connection_cmd += ["--conf", conf_el]
        if self._total_executor_cores:
            connection_cmd += ["--total-executor-cores", str(self._total_executor_cores)]
        if self._executor_cores:
            connection_cmd += ["--executor-cores", str(self._executor_cores)]
        if self._executor_memory:
            connection_cmd += ["--executor-memory", self._executor_memory]
        if self._keytab:
            connection_cmd += ["--keytab", self._keytab]
        if self._principal:
            connection_cmd += ["--principal", self._principal]
        if self._num_executors:
            connection_cmd += ["--num-executors", str(self._num_executors)]
        if self._sql:
            sql = self._sql.strip()
            if sql.endswith(".sql") or sql.endswith(".hql"):
                connection_cmd += ["-f", sql]
            else:
                connection_cmd += ["-e", sql]
        if self._master:
            connection_cmd += ["--master", self._master]
        if self._name:
            connection_cmd += ["--name", self._name]
        if self._verbose:
            connection_cmd += ["--verbose"]
        if self._yarn_queue:
            connection_cmd += ["--queue", self._yarn_queue]

        if isinstance(cmd, str):
            connection_cmd += cmd.split()
        elif isinstance(cmd, list):
            connection_cmd += cmd
        else:
            raise AirflowException(f"Invalid additional command: {cmd}")

        self.log.debug("Spark-Sql cmd: %s", connection_cmd)

        return connection_cmd

    def run_query(self, cmd: str = "", **kwargs: Any) -> None:
        """
        Remote Popen (actually execute the Spark-sql query)

        :param cmd: command to append to the spark-sql command
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        spark_sql_cmd = self._prepare_command(cmd)

        self._sp = subprocess.Popen(
            spark_sql_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, universal_newlines=True, **kwargs
        )

        for line in iter(self._sp.stdout):  # type: ignore
            self.log.info(line)

        returncode = self._sp.wait()

        if returncode:
            raise AirflowException(
                f"Cannot execute '{self._sql}' on {self._master} (additional parameters: '{cmd}'). "
                f"Process exit code: {returncode}."
            )

    def kill(self) -> None:
        """Kill Spark job"""
        if self._sp and self._sp.poll() is None:
            self.log.info("Killing the Spark-Sql job")
            self._sp.kill()
