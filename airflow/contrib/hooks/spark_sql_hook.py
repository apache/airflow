# -*- coding: utf-8 -*-
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
#
import subprocess

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SparkSqlHook(BaseHook):
    """
    This hook is a wrapper around the spark-sql binary. It requires that the
    "spark-sql" binary is in the PATH.

    :param sql: The SQL query to execute
    :type sql: str
    :param conf: arbitrary Spark configuration property
    :type conf: str (format: PROP=VALUE)
    :param conn_id: connection_id string
    :type conn_id: str
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
        (Default: all the available cores on the worker)
    :type total_executor_cores: int
    :param executor_cores: (Standalone & YARN only) Number of cores per
        executor (Default: 2)
    :type executor_cores: int
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :type executor_memory: str
    :param keytab: Full path to the file that contains the keytab
    :type keytab: str
    :param master: spark://host:port, mesos://host:port, yarn, or local
    :type master: str
    :param name: Name of the job.
    :type name: str
    :param num_executors: Number of executors to launch
    :type num_executors: int
    :param verbose: Whether to pass the verbose flag to spark-sql
    :type verbose: bool
    :param yarn_queue: The YARN queue to submit to (Default: "default")
    :type yarn_queue: str
    """
    def __init__(self,
                 sql,
                 conf=None,
                 conn_id='spark_sql_default',
                 total_executor_cores=None,
                 executor_cores=None,
                 executor_memory=None,
                 keytab=None,
                 principal=None,
                 master='yarn',
                 name='default-name',
                 num_executors=None,
                 verbose=True,
                 yarn_queue='default'
                 ):
        self._sql = sql
        self._conf = conf
        self._conn = self.get_connection(conn_id)
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
        self._sp = None

    def get_conn(self):
        pass

    def _prepare_command(self, cmd):
        """
        Construct the spark-sql command to execute. Verbose output is enabled
        as default.

        :param cmd: command to append to the spark-sql command
        :type cmd: str
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

        connection_cmd += cmd
        self.log.debug("Spark-Sql cmd: %s", connection_cmd)

        return connection_cmd

    def run_query(self, cmd="", **kwargs):
        """
        Remote Popen (actually execute the Spark-sql query)

        :param cmd: command to remotely execute
        :param kwargs: extra arguments to Popen (see subprocess.Popen)
        """
        spark_sql_cmd = self._prepare_command(cmd)
        self._sp = subprocess.Popen(spark_sql_cmd,
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.STDOUT,
                                    **kwargs)

        for line in iter(self._sp.stdout.readline, ''):
            self.log.info(line)

        returncode = self._sp.wait()

        if returncode:
            raise AirflowException(
                "Cannot execute {} on {}. Process exit code: {}.".format(
                    cmd, self._conn.host, returncode
                )
            )

    def kill(self):
        if self._sp and self._sp.poll() is None:
            self.log.info("Killing the Spark-Sql job")
            self._sp.kill()
