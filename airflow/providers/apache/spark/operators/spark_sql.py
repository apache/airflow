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
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_sql import SparkSqlHook
from airflow.utils.decorators import apply_defaults


class SparkSqlOperator(BaseOperator):
    """
    Execute Spark SQL query

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkSqlOperator`

    :param sql: The SQL query to execute. (templated)
    :type sql: str
    :param conf: arbitrary Spark configuration property
    :type conf: str (format: PROP=VALUE)
    :param conn_id: connection_id string
    :type conn_id: str
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all
        executors (Default: all the available cores on the worker)
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
    :param name: Name of the job
    :type name: str
    :param num_executors: Number of executors to launch
    :type num_executors: int
    :param verbose: Whether to pass the verbose flag to spark-sql
    :type verbose: bool
    :param yarn_queue: The YARN queue to submit to (Default: "default")
    :type yarn_queue: str
    """

    template_fields = ["_sql"]
    template_ext = [".sql", ".hql"]

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(self,
                 sql: str,
                 conf: Optional[str] = None,
                 conn_id: str = 'spark_sql_default',
                 total_executor_cores: Optional[int] = None,
                 executor_cores: Optional[int] = None,
                 executor_memory: Optional[str] = None,
                 keytab: Optional[str] = None,
                 principal: Optional[str] = None,
                 master: str = 'yarn',
                 name: str = 'default-name',
                 num_executors: Optional[int] = None,
                 verbose: bool = True,
                 yarn_queue: str = 'default',
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._sql = sql
        self._conf = conf
        self._conn_id = conn_id
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
        self._hook: Optional[SparkSqlHook] = None

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Call the SparkSqlHook to run the provided sql query
        """
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.run_query()

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.kill()

    def _get_hook(self) -> SparkSqlHook:
        """ Get SparkSqlHook """
        return SparkSqlHook(sql=self._sql,
                            conf=self._conf,
                            conn_id=self._conn_id,
                            total_executor_cores=self._total_executor_cores,
                            executor_cores=self._executor_cores,
                            executor_memory=self._executor_memory,
                            keytab=self._keytab,
                            principal=self._principal,
                            name=self._name,
                            num_executors=self._num_executors,
                            master=self._master,
                            verbose=self._verbose,
                            yarn_queue=self._yarn_queue
                            )
