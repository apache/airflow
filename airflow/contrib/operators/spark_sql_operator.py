# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.spark_sql_hook import SparkSqlHook


class SparkSqlOperator(BaseOperator):
    """
    Execute Spark SQL query
    :param sql: The SQL query to execute
    :type sql: str
    :param conf: arbitrary Spark configuration property
    :type conf: str (format: PROP=VALUE)
    :param conn_id: connection_id string
    :type conn_id: str
    :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors (Default: all the available cores on the worker)
    :type total_executor_cores: int
    :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
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

    @apply_defaults
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
                 yarn_queue='default',
                 *args,
                 **kwargs):
        super(SparkSqlOperator, self).__init__(*args, **kwargs)
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
        self._yarn_queue = yarn_queue
        self._hook = None

    def execute(self, context):
        """
        Call the SparkSqlHook to run the provided sql query
        """
        self._hook = SparkSqlHook(sql=self._sql,
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
                                  yarn_queue=self._yarn_queue
                                  )
        self._hook.run_query()

    def on_kill(self):
        self._hook.kill()
