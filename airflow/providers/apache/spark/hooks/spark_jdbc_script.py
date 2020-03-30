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
import argparse

from pyspark.sql import SparkSession


def set_common_options(spark_source,
                       url='localhost:5432',
                       jdbc_table='default.default',
                       user='root',
                       password='root',
                       driver='driver'):
    """
    Get Spark source from JDBC connection

    :param spark_source: Spark source, here is Spark reader or writer
    :param url: JDBC resource url
    :param jdbc_table: JDBC resource table name
    :param user: JDBC resource user name
    :param password: JDBC resource password
    :param driver: JDBC resource driver
    """

    spark_source = spark_source \
        .format('jdbc') \
        .option('url', url) \
        .option('dbtable', jdbc_table) \
        .option('user', user) \
        .option('password', password) \
        .option('driver', driver)
    return spark_source


# pylint: disable=too-many-arguments
def spark_write_to_jdbc(spark_session, url, user, password, metastore_table, jdbc_table, driver,
                        truncate, save_mode, batch_size, num_partitions,
                        create_table_column_types):
    """
    Transfer data from Spark to JDBC source
    """
    writer = spark_session \
        .table(metastore_table) \
        .write \

    # first set common options
    writer = set_common_options(writer, url, jdbc_table, user, password, driver)

    # now set write-specific options
    if truncate:
        writer = writer.option('truncate', truncate)
    if batch_size:
        writer = writer.option('batchsize', batch_size)
    if num_partitions:
        writer = writer.option('numPartitions', num_partitions)
    if create_table_column_types:
        writer = writer.option("createTableColumnTypes", create_table_column_types)

    writer \
        .save(mode=save_mode)


# pylint: disable=too-many-arguments
def spark_read_from_jdbc(spark_session, url, user, password, metastore_table, jdbc_table, driver,
                         save_mode, save_format, fetch_size, num_partitions,
                         partition_column, lower_bound, upper_bound):
    """
    Transfer data from JDBC source to Spark
    """

    # first set common options
    reader = set_common_options(spark_session.read, url, jdbc_table, user, password, driver)

    # now set specific read options
    if fetch_size:
        reader = reader.option('fetchsize', fetch_size)
    if num_partitions:
        reader = reader.option('numPartitions', num_partitions)
    if partition_column and lower_bound and upper_bound:
        reader = reader \
            .option('partitionColumn', partition_column) \
            .option('lowerBound', lower_bound) \
            .option('upperBound', upper_bound)

    reader \
        .load() \
        .write \
        .saveAsTable(metastore_table, format=save_format, mode=save_mode)


if __name__ == "__main__":  # pragma: no cover
    # parse the parameters
    parser = argparse.ArgumentParser(description='Spark-JDBC')
    parser.add_argument('-cmdType', dest='cmd_type', action='store')
    parser.add_argument('-url', dest='url', action='store')
    parser.add_argument('-user', dest='user', action='store')
    parser.add_argument('-password', dest='password', action='store')
    parser.add_argument('-metastoreTable', dest='metastore_table', action='store')
    parser.add_argument('-jdbcTable', dest='jdbc_table', action='store')
    parser.add_argument('-jdbcDriver', dest='jdbc_driver', action='store')
    parser.add_argument('-jdbcTruncate', dest='truncate', action='store')
    parser.add_argument('-saveMode', dest='save_mode', action='store')
    parser.add_argument('-saveFormat', dest='save_format', action='store')
    parser.add_argument('-batchsize', dest='batch_size', action='store')
    parser.add_argument('-fetchsize', dest='fetch_size', action='store')
    parser.add_argument('-name', dest='name', action='store')
    parser.add_argument('-numPartitions', dest='num_partitions', action='store')
    parser.add_argument('-partitionColumn', dest='partition_column', action='store')
    parser.add_argument('-lowerBound', dest='lower_bound', action='store')
    parser.add_argument('-upperBound', dest='upper_bound', action='store')
    parser.add_argument('-createTableColumnTypes',
                        dest='create_table_column_types', action='store')
    arguments = parser.parse_args()

    # Disable dynamic allocation by default to allow num_executors to take effect.
    spark = SparkSession.builder \
        .appName(arguments.name) \
        .enableHiveSupport() \
        .getOrCreate()

    if arguments.cmd_type == "spark_to_jdbc":
        spark_write_to_jdbc(spark,
                            arguments.url,
                            arguments.user,
                            arguments.password,
                            arguments.metastore_table,
                            arguments.jdbc_table,
                            arguments.jdbc_driver,
                            arguments.truncate,
                            arguments.save_mode,
                            arguments.batch_size,
                            arguments.num_partitions,
                            arguments.create_table_column_types)
    elif arguments.cmd_type == "jdbc_to_spark":
        spark_read_from_jdbc(spark,
                             arguments.url,
                             arguments.user,
                             arguments.password,
                             arguments.metastore_table,
                             arguments.jdbc_table,
                             arguments.jdbc_driver,
                             arguments.save_mode,
                             arguments.save_format,
                             arguments.fetch_size,
                             arguments.num_partitions,
                             arguments.partition_column,
                             arguments.lower_bound,
                             arguments.upper_bound)
