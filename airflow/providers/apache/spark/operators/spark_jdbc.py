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

from typing import TYPE_CHECKING, Any

from airflow.providers.apache.spark.hooks.spark_jdbc import SparkJDBCHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SparkJDBCOperator(SparkSubmitOperator):
    """
    This operator extends the SparkSubmitOperator specifically for performing data
    transfers to/from JDBC-based databases with Apache Spark. As with the
    SparkSubmitOperator, it assumes that the "spark-submit" binary is available on the
    PATH.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SparkJDBCOperator`

    :param spark_app_name: Name of the job (default airflow-spark-jdbc)
    :param spark_conn_id: The :ref:`spark connection id <howto/connection:spark>`
        as configured in Airflow administration
    :param spark_conf: Any additional Spark configuration properties
    :param spark_py_files: Additional python files used (.zip, .egg, or .py)
    :param spark_files: Additional files to upload to the container running the job
    :param spark_jars: Additional jars to upload and add to the driver and
                       executor classpath
    :param num_executors: number of executor to run. This should be set so as to manage
                          the number of connections made with the JDBC database
    :param executor_cores: Number of cores per executor
    :param executor_memory: Memory per executor (e.g. 1000M, 2G)
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G)
    :param verbose: Whether to pass the verbose flag to spark-submit for debugging
    :param keytab: Full path to the file that contains the keytab
    :param principal: The name of the kerberos principal used for keytab
    :param cmd_type: Which way the data should flow. 2 possible values:
                     spark_to_jdbc: data written by spark from metastore to jdbc
                     jdbc_to_spark: data written by spark from jdbc to metastore
    :param jdbc_table: The name of the JDBC table
    :param jdbc_conn_id: Connection id used for connection to JDBC database
    :param jdbc_driver: Name of the JDBC driver to use for the JDBC connection. This
                        driver (usually a jar) should be passed in the 'jars' parameter
    :param metastore_table: The name of the metastore table,
    :param jdbc_truncate: (spark_to_jdbc only) Whether or not Spark should truncate or
                         drop and recreate the JDBC table. This only takes effect if
                         'save_mode' is set to Overwrite. Also, if the schema is
                         different, Spark cannot truncate, and will drop and recreate
    :param save_mode: The Spark save-mode to use (e.g. overwrite, append, etc.)
    :param save_format: (jdbc_to_spark-only) The Spark save-format to use (e.g. parquet)
    :param batch_size: (spark_to_jdbc only) The size of the batch to insert per round
                       trip to the JDBC database. Defaults to 1000
    :param fetch_size: (jdbc_to_spark only) The size of the batch to fetch per round trip
                       from the JDBC database. Default depends on the JDBC driver
    :param num_partitions: The maximum number of partitions that can be used by Spark
                           simultaneously, both for spark_to_jdbc and jdbc_to_spark
                           operations. This will also cap the number of JDBC connections
                           that can be opened
    :param partition_column: (jdbc_to_spark-only) A numeric column to be used to
                             partition the metastore table by. If specified, you must
                             also specify:
                             num_partitions, lower_bound, upper_bound
    :param lower_bound: (jdbc_to_spark-only) Lower bound of the range of the numeric
                        partition column to fetch. If specified, you must also specify:
                        num_partitions, partition_column, upper_bound
    :param upper_bound: (jdbc_to_spark-only) Upper bound of the range of the numeric
                        partition column to fetch. If specified, you must also specify:
                        num_partitions, partition_column, lower_bound
    :param create_table_column_types: (spark_to_jdbc-only) The database column data types
                                      to use instead of the defaults, when creating the
                                      table. Data type information should be specified in
                                      the same format as CREATE TABLE columns syntax
                                      (e.g: "name CHAR(64), comments VARCHAR(1024)").
                                      The specified types should be valid spark sql data
                                      types.
    """

    def __init__(
        self,
        *,
        spark_app_name: str = "airflow-spark-jdbc",
        spark_conn_id: str = "spark-default",
        spark_conf: dict[str, Any] | None = None,
        spark_py_files: str | None = None,
        spark_files: str | None = None,
        spark_jars: str | None = None,
        num_executors: int | None = None,
        executor_cores: int | None = None,
        executor_memory: str | None = None,
        driver_memory: str | None = None,
        verbose: bool = False,
        principal: str | None = None,
        keytab: str | None = None,
        cmd_type: str = "spark_to_jdbc",
        jdbc_table: str | None = None,
        jdbc_conn_id: str = "jdbc-default",
        jdbc_driver: str | None = None,
        metastore_table: str | None = None,
        jdbc_truncate: bool = False,
        save_mode: str | None = None,
        save_format: str | None = None,
        batch_size: int | None = None,
        fetch_size: int | None = None,
        num_partitions: int | None = None,
        partition_column: str | None = None,
        lower_bound: str | None = None,
        upper_bound: str | None = None,
        create_table_column_types: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._spark_app_name = spark_app_name
        self._spark_conn_id = spark_conn_id
        self._spark_conf = spark_conf
        self._spark_py_files = spark_py_files
        self._spark_files = spark_files
        self._spark_jars = spark_jars
        self._num_executors = num_executors
        self._executor_cores = executor_cores
        self._executor_memory = executor_memory
        self._driver_memory = driver_memory
        self._verbose = verbose
        self._keytab = keytab
        self._principal = principal
        self._cmd_type = cmd_type
        self._jdbc_table = jdbc_table
        self._jdbc_conn_id = jdbc_conn_id
        self._jdbc_driver = jdbc_driver
        self._metastore_table = metastore_table
        self._jdbc_truncate = jdbc_truncate
        self._save_mode = save_mode
        self._save_format = save_format
        self._batch_size = batch_size
        self._fetch_size = fetch_size
        self._num_partitions = num_partitions
        self._partition_column = partition_column
        self._lower_bound = lower_bound
        self._upper_bound = upper_bound
        self._create_table_column_types = create_table_column_types
        self._hook: SparkJDBCHook | None = None

    def execute(self, context: Context) -> None:
        """Call the SparkSubmitHook to run the provided spark job"""
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.submit_jdbc_job()

    def on_kill(self) -> None:
        if self._hook is None:
            self._hook = self._get_hook()
        self._hook.on_kill()

    def _get_hook(self) -> SparkJDBCHook:
        return SparkJDBCHook(
            spark_app_name=self._spark_app_name,
            spark_conn_id=self._spark_conn_id,
            spark_conf=self._spark_conf,
            spark_py_files=self._spark_py_files,
            spark_files=self._spark_files,
            spark_jars=self._spark_jars,
            num_executors=self._num_executors,
            executor_cores=self._executor_cores,
            executor_memory=self._executor_memory,
            driver_memory=self._driver_memory,
            verbose=self._verbose,
            keytab=self._keytab,
            principal=self._principal,
            cmd_type=self._cmd_type,
            jdbc_table=self._jdbc_table,
            jdbc_conn_id=self._jdbc_conn_id,
            jdbc_driver=self._jdbc_driver,
            metastore_table=self._metastore_table,
            jdbc_truncate=self._jdbc_truncate,
            save_mode=self._save_mode,
            save_format=self._save_format,
            batch_size=self._batch_size,
            fetch_size=self._fetch_size,
            num_partitions=self._num_partitions,
            partition_column=self._partition_column,
            lower_bound=self._lower_bound,
            upper_bound=self._upper_bound,
            create_table_column_types=self._create_table_column_types,
        )
