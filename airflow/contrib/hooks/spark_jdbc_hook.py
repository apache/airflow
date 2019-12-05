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
import os
from airflow.contrib.hooks.spark_submit_hook import SparkSubmitHook
from airflow.exceptions import AirflowException


class SparkJDBCHook(SparkSubmitHook):
    """
    This hook extends the SparkSubmitHook specifically for performing data
    transfers to/from JDBC-based databases with Apache Spark.

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
    def __init__(self,
                 spark_app_name='airflow-spark-jdbc',
                 spark_conn_id='spark-default',
                 spark_conf=None,
                 spark_py_files=None,
                 spark_files=None,
                 spark_jars=None,
                 num_executors=None,
                 executor_cores=None,
                 executor_memory=None,
                 driver_memory=None,
                 verbose=False,
                 principal=None,
                 keytab=None,
                 cmd_type='spark_to_jdbc',
                 jdbc_table=None,
                 jdbc_conn_id='jdbc-default',
                 jdbc_driver=None,
                 metastore_table=None,
                 jdbc_truncate=False,
                 save_mode=None,
                 save_format=None,
                 batch_size=None,
                 fetch_size=None,
                 num_partitions=None,
                 partition_column=None,
                 lower_bound=None,
                 upper_bound=None,
                 create_table_column_types=None,
                 *args,
                 **kwargs
                 ):
        super(SparkJDBCHook, self).__init__(*args, **kwargs)
        self._name = spark_app_name
        self._conn_id = spark_conn_id
        self._conf = spark_conf
        self._py_files = spark_py_files
        self._files = spark_files
        self._jars = spark_jars
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
        self._jdbc_connection = self._resolve_jdbc_connection()

    def _resolve_jdbc_connection(self):
        conn_data = {'url': '',
                     'schema': '',
                     'conn_prefix': '',
                     'user': '',
                     'password': ''
                     }
        try:
            conn = self.get_connection(self._jdbc_conn_id)
            if conn.port:
                conn_data['url'] = "{}:{}".format(conn.host, conn.port)
            else:
                conn_data['url'] = conn.host
            conn_data['schema'] = conn.schema
            conn_data['user'] = conn.login
            conn_data['password'] = conn.password
            extra = conn.extra_dejson
            conn_data['conn_prefix'] = extra.get('conn_prefix', '')
        except AirflowException:
            self.log.debug(
                "Could not load jdbc connection string %s, defaulting to %s",
                self._jdbc_conn_id, ""
            )
        return conn_data

    def _build_jdbc_application_arguments(self, jdbc_conn):
        arguments = []
        arguments += ["-cmdType", self._cmd_type]
        if self._jdbc_connection['url']:
            arguments += ['-url', "{0}{1}/{2}".format(
                jdbc_conn['conn_prefix'], jdbc_conn['url'], jdbc_conn['schema']
            )]
        if self._jdbc_connection['user']:
            arguments += ['-user', self._jdbc_connection['user']]
        if self._jdbc_connection['password']:
            arguments += ['-password', self._jdbc_connection['password']]
        if self._metastore_table:
            arguments += ['-metastoreTable', self._metastore_table]
        if self._jdbc_table:
            arguments += ['-jdbcTable', self._jdbc_table]
        if self._jdbc_truncate:
            arguments += ['-jdbcTruncate', str(self._jdbc_truncate)]
        if self._jdbc_driver:
            arguments += ['-jdbcDriver', self._jdbc_driver]
        if self._batch_size:
            arguments += ['-batchsize', str(self._batch_size)]
        if self._fetch_size:
            arguments += ['-fetchsize', str(self._fetch_size)]
        if self._num_partitions:
            arguments += ['-numPartitions', str(self._num_partitions)]
        if (self._partition_column and self._lower_bound and
                self._upper_bound and self._num_partitions):
            # these 3 parameters need to be used all together to take effect.
            arguments += ['-partitionColumn', self._partition_column,
                          '-lowerBound', self._lower_bound,
                          '-upperBound', self._upper_bound]
        if self._save_mode:
            arguments += ['-saveMode', self._save_mode]
        if self._save_format:
            arguments += ['-saveFormat', self._save_format]
        if self._create_table_column_types:
            arguments += ['-createTableColumnTypes', self._create_table_column_types]
        return arguments

    def submit_jdbc_job(self):
        self._application_args = \
            self._build_jdbc_application_arguments(self._jdbc_connection)
        self.submit(application=os.path.dirname(os.path.abspath(__file__)) +
                    "/spark_jdbc_script.py")

    def get_conn(self):
        pass
