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

import os
import warnings
from contextlib import closing
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Iterable, Union

from deprecated import deprecated
from sqlalchemy.engine import URL
#import ydb_sqlalchemy.dbapi.connection as YDBConnection
from airflow.providers.ydb.hooks.dbapi.connection import Connection
from airflow.providers.ydb.hooks.dbapi.cursor import YdbQuery
import ydb
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

class YDBCursor:
    def __init__(self, delegatee, is_ddl):
        self.delegatee = delegatee
        self.is_ddl = is_ddl

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None):
        q = YdbQuery(yql_text=sql, is_ddl=self.is_ddl)
        return self.delegatee.execute(q, parameters)

    def executemany(self, sql: str, seq_of_parameters: Optional[Sequence[Mapping[str, Any]]]):
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def executescript(self, script):
        return self.execute(script)

    def fetchone(self):
        return self.delegatee.fetchone()

    def fetchmany(self, size=None):
        return self.delegatee.fetchmany(size=size)

    def fetchall(self):
        return self.delegatee.fetchall()

    def nextset(self):
        return self.delegatee.nextset()

    def setinputsizes(self, sizes):
        return self.delegatee.setinputsizes()

    def setoutputsize(self, column=None):
        return self.delegatee.setoutputsize()

    def close(self):
        return self.delegatee.close()

    @property
    def rowcount(self):
        return self.delegatee.rowcount

    @property
    def description(self):
        # hack
        return "ZZZ"


class YDBConnection:
    def __init__(self, is_ddl):
        self.is_ddl = is_ddl
        endpoint = "grpcs://ydb.serverless.yandexcloud.net:2135"
        database = "/ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b"
        iam_token = "t1.9..."
        credentials = ydb.AccessTokenCredentials(iam_token)
        driver_config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            table_client_settings=self._get_table_client_settings(),
            credentials=credentials,
        )
        driver = ydb.Driver(driver_config)
        # wait until driver become initialized
        driver.wait(fail_fast=True, timeout=10)
        ydb_session_pool = ydb.SessionPool(driver, size=5)
        self.delegatee = Connection(ydb_session_pool=ydb_session_pool)

    def cursor(self):
        return YDBCursor(self.delegatee.cursor(), is_ddl=self.is_ddl)

    def begin(self):
        self.delegatee.begin()

    def commit(self):
        self.delegatee.commit()

    def rollback(self):
        self.delegatee.rollback()

    def close(self):
        self.delegatee.close()

    def _get_table_client_settings(self) -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )


class YDBHook(DbApiHook):
    """Interact with YDB.

    """

    conn_name_attr = "ydb_conn_id"
    default_conn_name = "ydb_default"
    conn_type = "ydb"
    hook_name = "YDB"
    supports_autocommit = True
    supports_executemany = True

    def __init__(self, *args, is_ddl: bool = False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.is_ddl = is_ddl
        self.conn: YDBConnection = None

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        return URL.create(
            drivername="postgresql",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=self.database or conn.schema,
        )

    def get_conn(self) -> YDBConnection:
        """Establish a connection to a YDB database."""
        self.conn = YDBConnection(is_ddl=self.is_ddl)
        return self.conn

    def get_uri(self) -> str:
        """Extract the URI from the connection.

        :return: the extracted URI in Sqlalchemy URI format.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    @staticmethod
    def _serialize_cell(cell: object, conn: YDBConnection | None = None) -> Any:
        return cell
