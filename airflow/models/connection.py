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

import json
from builtins import bytes
from urllib.parse import urlparse, unquote, parse_qsl

from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import synonym

from airflow import LoggingMixin
from airflow.exceptions import AirflowException
from airflow.models.base import Base, ID_LEN
from airflow.models.crypto import get_fernet


# Python automatically converts all letters to lowercase in hostname
# See: https://issues.apache.org/jira/browse/AIRFLOW-3615
def parse_netloc_to_hostname(uri_parts):
    hostname = unquote(uri_parts.hostname or '')
    if '/' in hostname:
        hostname = uri_parts.netloc
        if "@" in hostname:
            hostname = hostname.rsplit("@", 1)[1]
        if ":" in hostname:
            hostname = hostname.split(":", 1)[0]
        hostname = unquote(hostname)
    return hostname


class Connection(Base, LoggingMixin):
    """
    Placeholder to store information about different database instances
    connection information. The idea here is that scripts use references to
    database instances (conn_id) instead of hard coding hostname, logins and
    passwords when using operators or hooks.
    """
    __tablename__ = "connection"

    id = Column(Integer(), primary_key=True)
    conn_id = Column(String(ID_LEN))
    conn_type = Column(String(500))
    host = Column(String(500))
    schema = Column(String(500))
    login = Column(String(500))
    _password = Column('password', String(5000))
    port = Column(Integer())
    is_encrypted = Column(Boolean, unique=False, default=False)
    is_extra_encrypted = Column(Boolean, unique=False, default=False)
    _extra = Column('extra', String(5000))

    _types = [
        ('docker', 'Docker Registry',),
        ('fs', 'File (path)'),
        ('ftp', 'FTP',),
        ('google_cloud_platform', 'Google Cloud Platform'),
        ('hdfs', 'HDFS',),
        ('http', 'HTTP',),
        ('pig_cli', 'Pig Client Wrapper',),
        ('hive_cli', 'Hive Client Wrapper',),
        ('hive_metastore', 'Hive Metastore Thrift',),
        ('hiveserver2', 'Hive Server 2 Thrift',),
        ('jdbc', 'Jdbc Connection',),
        ('jenkins', 'Jenkins'),
        ('mysql', 'MySQL',),
        ('postgres', 'Postgres',),
        ('oracle', 'Oracle',),
        ('vertica', 'Vertica',),
        ('presto', 'Presto',),
        ('s3', 'S3',),
        ('samba', 'Samba',),
        ('sqlite', 'Sqlite',),
        ('ssh', 'SSH',),
        ('cloudant', 'IBM Cloudant',),
        ('mssql', 'Microsoft SQL Server'),
        ('mesos_framework-id', 'Mesos Framework ID'),
        ('jira', 'JIRA',),
        ('redis', 'Redis',),
        ('wasb', 'Azure Blob Storage'),
        ('databricks', 'Databricks',),
        ('aws', 'Amazon Web Services',),
        ('emr', 'Elastic MapReduce',),
        ('snowflake', 'Snowflake',),
        ('segment', 'Segment',),
        ('sqoop', 'Sqoop',),
        ('azure_data_lake', 'Azure Data Lake'),
        ('azure_container_instances', 'Azure Container Instances'),
        ('azure_cosmos', 'Azure CosmosDB'),
        ('cassandra', 'Cassandra',),
        ('qubole', 'Qubole'),
        ('mongo', 'MongoDB'),
        ('gcpcloudsql', 'Google Cloud SQL'),
        ('grpc', 'GRPC Connection'),
    ]

    def __init__(
            self, conn_id=None, conn_type=None,
            host=None, login=None, password=None,
            schema=None, port=None, extra=None,
            uri=None):
        self.conn_id = conn_id
        if uri:
            self.parse_from_uri(uri)
        else:
            self.conn_type = conn_type
            self.host = host
            self.login = login
            self.password = password
            self.schema = schema
            self.port = port
            self.extra = extra

    def parse_from_uri(self, uri):
        uri_parts = urlparse(uri)
        conn_type = uri_parts.scheme
        if conn_type == 'postgresql':
            conn_type = 'postgres'
        elif '-' in conn_type:
            conn_type = conn_type.replace('-', '_')
        self.conn_type = conn_type
        self.host = parse_netloc_to_hostname(uri_parts)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = unquote(uri_parts.username) \
            if uri_parts.username else uri_parts.username
        self.password = unquote(uri_parts.password) \
            if uri_parts.password else uri_parts.password
        self.port = uri_parts.port
        if uri_parts.query:
            self.extra = json.dumps(dict(parse_qsl(uri_parts.query)))

    def get_password(self):
        if self._password and self.is_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    "Can't decrypt encrypted password for login={}, \
                    FERNET_KEY configuration is missing".format(self.login))
            return fernet.decrypt(bytes(self._password, 'utf-8')).decode()
        else:
            return self._password

    def set_password(self, value):
        if value:
            fernet = get_fernet()
            self._password = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def password(cls):
        return synonym('_password',
                       descriptor=property(cls.get_password, cls.set_password))

    def get_extra(self):
        if self._extra and self.is_extra_encrypted:
            fernet = get_fernet()
            if not fernet.is_encrypted:
                raise AirflowException(
                    "Can't decrypt `extra` params for login={},\
                    FERNET_KEY configuration is missing".format(self.login))
            return fernet.decrypt(bytes(self._extra, 'utf-8')).decode()
        else:
            return self._extra

    def set_extra(self, value):
        if value:
            fernet = get_fernet()
            self._extra = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_extra_encrypted = fernet.is_encrypted
        else:
            self._extra = value
            self.is_extra_encrypted = False

    @declared_attr
    def extra(cls):
        return synonym('_extra',
                       descriptor=property(cls.get_extra, cls.set_extra))

    def rotate_fernet_key(self):
        fernet = get_fernet()
        if self._password and self.is_encrypted:
            self._password = fernet.rotate(self._password.encode('utf-8')).decode()
        if self._extra and self.is_extra_encrypted:
            self._extra = fernet.rotate(self._extra.encode('utf-8')).decode()

    def get_hook(self):
        if self.conn_type == 'mysql':
            from airflow.hooks.mysql_hook import MySqlHook
            return MySqlHook(mysql_conn_id=self.conn_id)
        elif self.conn_type == 'google_cloud_platform':
            from airflow.contrib.hooks.bigquery_hook import BigQueryHook
            return BigQueryHook(bigquery_conn_id=self.conn_id)
        elif self.conn_type == 'postgres':
            from airflow.hooks.postgres_hook import PostgresHook
            return PostgresHook(postgres_conn_id=self.conn_id)
        elif self.conn_type == 'pig_cli':
            from airflow.hooks.pig_hook import PigCliHook
            return PigCliHook(pig_conn_id=self.conn_id)
        elif self.conn_type == 'hive_cli':
            from airflow.hooks.hive_hooks import HiveCliHook
            return HiveCliHook(hive_cli_conn_id=self.conn_id)
        elif self.conn_type == 'presto':
            from airflow.hooks.presto_hook import PrestoHook
            return PrestoHook(presto_conn_id=self.conn_id)
        elif self.conn_type == 'hiveserver2':
            from airflow.hooks.hive_hooks import HiveServer2Hook
            return HiveServer2Hook(hiveserver2_conn_id=self.conn_id)
        elif self.conn_type == 'sqlite':
            from airflow.hooks.sqlite_hook import SqliteHook
            return SqliteHook(sqlite_conn_id=self.conn_id)
        elif self.conn_type == 'jdbc':
            from airflow.hooks.jdbc_hook import JdbcHook
            return JdbcHook(jdbc_conn_id=self.conn_id)
        elif self.conn_type == 'mssql':
            from airflow.hooks.mssql_hook import MsSqlHook
            return MsSqlHook(mssql_conn_id=self.conn_id)
        elif self.conn_type == 'oracle':
            from airflow.hooks.oracle_hook import OracleHook
            return OracleHook(oracle_conn_id=self.conn_id)
        elif self.conn_type == 'vertica':
            from airflow.contrib.hooks.vertica_hook import VerticaHook
            return VerticaHook(vertica_conn_id=self.conn_id)
        elif self.conn_type == 'cloudant':
            from airflow.contrib.hooks.cloudant_hook import CloudantHook
            return CloudantHook(cloudant_conn_id=self.conn_id)
        elif self.conn_type == 'jira':
            from airflow.contrib.hooks.jira_hook import JiraHook
            return JiraHook(jira_conn_id=self.conn_id)
        elif self.conn_type == 'redis':
            from airflow.contrib.hooks.redis_hook import RedisHook
            return RedisHook(redis_conn_id=self.conn_id)
        elif self.conn_type == 'wasb':
            from airflow.contrib.hooks.wasb_hook import WasbHook
            return WasbHook(wasb_conn_id=self.conn_id)
        elif self.conn_type == 'docker':
            from airflow.hooks.docker_hook import DockerHook
            return DockerHook(docker_conn_id=self.conn_id)
        elif self.conn_type == 'azure_data_lake':
            from airflow.contrib.hooks.azure_data_lake_hook import AzureDataLakeHook
            return AzureDataLakeHook(azure_data_lake_conn_id=self.conn_id)
        elif self.conn_type == 'azure_cosmos':
            from airflow.contrib.hooks.azure_cosmos_hook import AzureCosmosDBHook
            return AzureCosmosDBHook(azure_cosmos_conn_id=self.conn_id)
        elif self.conn_type == 'cassandra':
            from airflow.contrib.hooks.cassandra_hook import CassandraHook
            return CassandraHook(cassandra_conn_id=self.conn_id)
        elif self.conn_type == 'mongo':
            from airflow.contrib.hooks.mongo_hook import MongoHook
            return MongoHook(conn_id=self.conn_id)
        elif self.conn_type == 'gcpcloudsql':
            from airflow.contrib.hooks.gcp_sql_hook import CloudSqlDatabaseHook
            return CloudSqlDatabaseHook(gcp_cloudsql_conn_id=self.conn_id)
        elif self.conn_type == 'grpc':
            from airflow.contrib.hooks.grpc_hook import GrpcHook
            return GrpcHook(grpc_conn_id=self.conn_id)
        raise AirflowException("Unknown hook type {}".format(self.conn_type))

    def __repr__(self):
        return self.conn_id

    def debug_info(self):
        return ("id: {}. Host: {}, Port: {}, Schema: {}, "
                "Login: {}, Password: {}, extra: {}".
                format(self.conn_id,
                       self.host,
                       self.port,
                       self.schema,
                       self.login,
                       "XXXXXXXX" if self.password else None,
                       self.extra_dejson))

    @property
    def extra_dejson(self):
        """Returns the extra property by deserializing json."""
        obj = {}
        if self.extra:
            try:
                obj = json.loads(self.extra)
            except Exception as e:
                self.log.exception(e)
                self.log.error("Failed parsing the json for conn_id %s", self.conn_id)

        return obj
