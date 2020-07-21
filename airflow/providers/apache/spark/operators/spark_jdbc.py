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

from airflow.providers.apache.spark.hooks.spark_jdbc import SparkJDBCHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.decorators import apply_defaults


# pylint: disable=too-many-instance-attributes
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
    :type spark_app_name: str
    :param spark_conn_id: Connection id as configured in Airflow administration
    :type spark_conn_id: str
    :param spark_conf: Any additional Spark configuration properties
    :type spark_conf: dict
    :param spark_py_files: Additional python files used (.zip, .egg, or .py)
    :type spark_py_files: str
    :param spark_files: Additional files to upload to the container running the job
    :type spark_files: str
    :param spark_jars: Additional jars to upload and add to the driver and
                       executor classpath
    :type spark_jars: str
    :param num_executors: number of executor to run. This should be set so as to manage
                          the number of connections made with the JDBC database
    :type num_executors: int
    :param executor_cores: Number of cores per executor
    :type executor_cores: int
    :param executor_memory: Memory per executor (e.g. 1000M, 2G)
    :type executor_memory: str
    :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G)
    :type driver_memory: str
    :param verbose: Whether to pass the verbose flag to spark-submit for debugging
    :type verbose: bool
    :param keytab: Full path to the file that contains the keytab
    :type keytab: str
    :param principal: The name of the kerberos principal used for keytab
    :type principal: str
    :param cmd_type: Which way the data should flow. 2 possible values:
                     spark_to_jdbc: data written by spark from metastore to jdbc
                     jdbc_to_spark: data written by spark from jdbc to metastore
    :type cmd_type: str
    :param jdbc_table: The name of the JDBC table
    :type jdbc_table: str
    :param jdbc_conn_id: Connection id used for connection to JDBC database
    :type jdbc_conn_id: str
    :param jdbc_driver: Name of the JDBC driver to use for the JDBC connection. This
                        driver (usually a jar) should be passed in the 'jars' parameter
    :type jdbc_driver: str
    :param metastore_table: The name of the metastore table,
    :type metastore_table: str
    :param jdbc_truncate: (spark_to_jdbc only) Whether or not Spark should truncate or
                         drop and recreate the JDBC table. This only takes effect if
                         'save_mode' is set to Overwrite. Also, if the schema is
                         different, Spark cannot truncate, and will drop and recreate
    :type jdbc_truncate: bool
    :param save_mode: The Spark save-mode to use (e.g. overwrite, append, etc.)
    :type save_mode: str
    :param save_format: (jdbc_to_spark-only) The Spark save-format to use (e.g. parquet)
    :type save_format: str
    :param batch_size: (spark_to_jdbc only) The size of the batch to insert per round
                       trip to the JDBC database. Defaults to 1000
    :type batch_size: int
    :param fetch_size: (jdbc_to_spark only) The size of the batch to fetch per round trip
                       from the JDBC database. Default depends on the JDBC driver
    :type fetch_size: int
    :param num_partitions: The maximum number of partitions that can be used by Spark
                           simultaneously, both for spark_to_jdbc and jdbc_to_spark
                           operations. This will also cap the number of JDBC connections
                           that can be opened
    :type num_partitions: int
    :param partition_column: (jdbc_to_spark-only) A numeric column to be used to
                             partition the metastore table by. If specified, you must
                             also specify:
                             num_partitions, lower_bound, upper_bound
    :type partition_column: str
    :param lower_bound: (jdbc_to_spark-only) Lower bound of the range of the numeric
                        partition column to fetch. If specified, you must also specify:
                        num_partitions, partition_column, upper_bound
    :type lower_bound: int
    :param upper_bound: (jdbc_to_spark-only) Upper bound of the range of the numeric
                        partition column to fetch. If specified, you must also specify:
                        num_partitions, partition_column, lower_bound
    :type upper_bound: int
    :param create_table_column_types: (spark_to_jdbc-only) The database column data types
                                      to use instead of the defaults, when creating the
                                      table. Data type information should be specified in
                                      the same format as CREATE TABLE columns syntax
                                      (e.g: "name CHAR(64), comments VARCHAR(1024)").
                                      The specified types should be valid spark sql data
                                      types.
    """

    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(self,
                 spark_app_name: str = 'airflow-spark-jdbc',
                 spark_conn_id: str = 'spark-default',
                 spark_conf: Optional[Dict[str, Any]] = None,
                 spark_py_files: Optional[str] = None,
                 spark_files: Optional[str] = None,
                 spark_jars: Optional[str] = None,
                 num_executors: Optional[int] = None,
                 executor_cores: Optional[int] = None,
                 executor_memory: Optional[str] = None,
                 driver_memory: Optional[str] = None,
                 verbose: bool = False,
                 principal: Optional[str] = None,
                 keytab: Optional[str] = None,
                 cmd_type: str = 'spark_to_jdbc',
                 jdbc_table: Optional[str] = None,
                 jdbc_conn_id: str = 'jdbc-default',
                 jdbc_driver: Optional[str] = None,
                 metastore_table: Optional[str] = None,
                 jdbc_truncate: bool = False,
                 save_mode: Optional[str] = None,
                 save_format: Optional[str] = None,
                 batch_size: Optional[int] = None,
                 fetch_size: Optional[int] = None,
                 num_partitions: Optional[int] = None,
                 partition_column: Optional[str] = None,
                 lower_bound: Optional[str] = None,
                 upper_bound: Optional[str] = None,
                 create_table_column_types: Optional[str] = None,
                 *args: Any,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
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
        self._hook: Optional[SparkJDBCHook] = None

    def execute(self, context: Dict[str, Any]) -> None:
        """
        Call the SparkSubmitHook to run the provided spark job
        """
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
            create_table_column_types=self._create_table_column_types
        )
