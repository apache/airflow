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
# fmt: off
"""Hooks."""
from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes = {
    'S3_hook': {
        'S3Hook': 'airflow.providers.amazon.aws.hooks.s3.S3Hook',
        'provide_bucket_name': 'airflow.providers.amazon.aws.hooks.s3.provide_bucket_name',
    },
    'base_hook': {
        'BaseHook': 'airflow.hooks.base.BaseHook',
    },
    'dbapi_hook': {
        'DbApiHook': 'airflow.providers.common.sql.hooks.sql.DbApiHook',
    },
    'docker_hook': {
        'DockerHook': 'airflow.providers.docker.hooks.docker.DockerHook',
    },
    'druid_hook': {
        'DruidDbApiHook': 'airflow.providers.apache.druid.hooks.druid.DruidDbApiHook',
        'DruidHook': 'airflow.providers.apache.druid.hooks.druid.DruidHook',
    },
    'hdfs_hook': {
        'HDFSHook': 'airflow.providers.apache.hdfs.hooks.hdfs.HDFSHook',
        'HDFSHookException': 'airflow.providers.apache.hdfs.hooks.hdfs.HDFSHookException',
    },
    'hive_hooks': {
        'HIVE_QUEUE_PRIORITIES': 'airflow.providers.apache.hive.hooks.hive.HIVE_QUEUE_PRIORITIES',
        'HiveCliHook': 'airflow.providers.apache.hive.hooks.hive.HiveCliHook',
        'HiveMetastoreHook': 'airflow.providers.apache.hive.hooks.hive.HiveMetastoreHook',
        'HiveServer2Hook': 'airflow.providers.apache.hive.hooks.hive.HiveServer2Hook',
    },
    'http_hook': {
        'HttpHook': 'airflow.providers.http.hooks.http.HttpHook',
    },
    'jdbc_hook': {
        'JdbcHook': 'airflow.providers.jdbc.hooks.jdbc.JdbcHook',
        'jaydebeapi': 'airflow.providers.jdbc.hooks.jdbc.jaydebeapi',
    },
    'mssql_hook': {
        'MsSqlHook': 'airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook',
    },
    'mysql_hook': {
        'MySqlHook': 'airflow.providers.mysql.hooks.mysql.MySqlHook',
    },
    'oracle_hook': {
        'OracleHook': 'airflow.providers.oracle.hooks.oracle.OracleHook',
    },
    'pig_hook': {
        'PigCliHook': 'airflow.providers.apache.pig.hooks.pig.PigCliHook',
    },
    'postgres_hook': {
        'PostgresHook': 'airflow.providers.postgres.hooks.postgres.PostgresHook',
    },
    'presto_hook': {
        'PrestoHook': 'airflow.providers.presto.hooks.presto.PrestoHook',
    },
    'samba_hook': {
        'SambaHook': 'airflow.providers.samba.hooks.samba.SambaHook',
    },
    'slack_hook': {
        'SlackHook': 'airflow.providers.slack.hooks.slack.SlackHook',
    },
    'sqlite_hook': {
        'SqliteHook': 'airflow.providers.sqlite.hooks.sqlite.SqliteHook',
    },
    'webhdfs_hook': {
        'WebHDFSHook': 'airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook',
    },
    'zendesk_hook': {
        'ZendeskHook': 'airflow.providers.zendesk.hooks.zendesk.ZendeskHook',
    },
}

add_deprecated_classes(__deprecated_classes, __name__)
