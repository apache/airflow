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

from typing import TYPE_CHECKING, Any, Sequence

from deprecated import deprecated

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.apache.spark.hooks.spark_sql import SparkSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkSqlOperator(BaseOperator):
    """
    Execute Spark SQL query.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkSqlOperator`

    :param sql: The SQL query to execute. (templated)
    :param conf: arbitrary Spark configuration property
    :param conn_id: connection_id string
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all
        executors (Default: all the available cores on the worker)
    :param executor_cores: (Standalone & YARN only) Number of cores per
        executor (Default: 2)
    :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
    :param keytab: Full path to the file that contains the keytab
    :param master: spark://host:port, mesos://host:port, yarn, or local
        (Default: The ``host`` and ``port`` set in the Connection, or ``"yarn"``)
    :param name: Name of the job
    :param num_executors: Number of executors to launch
    :param verbose: Whether to pass the verbose flag to spark-sql
    :param yarn_queue: The YARN queue to submit to
        (Default: The ``queue`` value set in the Connection, or ``"default"``)
    """

    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql", ".hql")
    template_fields_renderers = {"sql": "sql"}

    def __init__(
        self,
        *,
        sql: str,
        conf: dict[str, Any] | str | None = None,
        conn_id: str = "spark_sql_default",
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
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
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
        self._hook: SparkSqlHook | None = None

    @property
    @deprecated(
        reason="`_sql` is deprecated and will be removed in the future. Please use `sql` instead.",
        category=AirflowProviderDeprecationWarning,
    )
    def _sql(self):
        """Alias for ``sql``, used for compatibility (deprecated)."""
        return self.sql

    def execute(self, context: Context) -> None:
        """Call the SparkSqlHook to run the provided sql query."""
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.run_query()

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.kill()

    def _get_hook(self) -> SparkSqlHook:
        """Get SparkSqlHook."""
        return SparkSqlHook(
            sql=self.sql,
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
            yarn_queue=self._yarn_queue,
        )
