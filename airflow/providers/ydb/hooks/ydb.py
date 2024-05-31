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

# import ydb_sqlalchemy.dbapi.connection as YDBConnection
from airflow.providers.ydb.hooks.dbapi.connection import Connection
from airflow.providers.ydb.hooks.dbapi.cursor import YdbQuery
from airflow.providers.ydb.utils.credentials import get_credentials_from_connection

import ydb
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

DEFAULT_YDB_GRPCS_PORT = 2135


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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        return self.delegatee.close()

    @property
    def rowcount(self):
        return self.delegatee.rowcount

    @property
    def description(self):
        return self.delegatee.description


class YDBConnection:
    def __init__(self, endpoint, database, credentials, is_ddl):
        self.is_ddl = is_ddl
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

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
    """Interact with YDB."""

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

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to YDB connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField, StringField, BooleanField

        return {
            "database": StringField(
                lazy_gettext("Database name"),
                widget=BS3PasswordFieldWidget(),
                description="Required. YDB database name",
            ),
            "service_account_json": PasswordField(
                lazy_gettext("Service account auth JSON"),
                widget=BS3PasswordFieldWidget(),
                description="Service account auth JSON. Looks like "
                '{"id": "...", "service_account_id": "...", "private_key": "..."}. '
                "Will be used instead of IAM token and SA JSON file path field if specified.",
            ),
            "service_account_json_path": StringField(
                lazy_gettext("Service account auth JSON file path"),
                widget=BS3TextFieldWidget(),
                description="Service account auth JSON file path. File content looks like "
                '{"id": "...", "service_account_id": "...", "private_key": "..."}. ',
            ),
            "token": PasswordField(
                lazy_gettext("IAM token"),
                widget=BS3PasswordFieldWidget(),
                description="User account IAM token. ",
            ),
            "use_vm_metadata": BooleanField(
                lazy_gettext("Use VM metadata"),
                default=False,
                description="Optional. Whether to use VM metadata to retrieve IAM token",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for YDB connection."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
            "placeholders": {
                "host": "eg. grpcs://my_host or ydb.serverless.yandexcloud.net or lb.etn9txxxx.ydb.mdb.yandexcloud.net",
                "login": "root",
                "password": "my_password",
                "database": "e.g. local or /ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b",
                "service_account_json": 'e.g. {"id": "...", "service_account_id": "...", "private_key": "..."}',
                "token": "t1.9....AAQ",
            },
        }

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        return URL.create(
            drivername="ydb",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=self.database,
        )

    def get_conn(self) -> YDBConnection:
        """Establish a connection to a YDB database."""
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        host = conn.host
        if not host:
            raise ValueError("YDB host must be specified")
        port = conn.port or DEFAULT_YDB_GRPCS_PORT
        endpoint = f"{host}:{port}"
        connection_extra = conn.extra_dejson
        database = connection_extra.get(
            "database"
        )  # "/ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b"
        if not database:
            raise ValueError("YDB database must be specified")

        credentials = get_credentials_from_connection(
            endpoint=endpoint, database=database, connection=conn, connection_extra=connection_extra
        )
        self.conn = YDBConnection(
            endpoint=endpoint, database=database, credentials=credentials, is_ddl=self.is_ddl
        )
        return self.conn

    def get_uri(self) -> str:
        """Extract the URI from the connection.

        :return: the extracted URI in Sqlalchemy URI format.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    @staticmethod
    def _serialize_cell(cell: object, conn: YDBConnection | None = None) -> Any:
        return cell
