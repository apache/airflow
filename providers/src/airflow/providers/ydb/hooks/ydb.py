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

from typing import TYPE_CHECKING, Any, Mapping, Sequence

import ydb
from sqlalchemy.engine import URL

from airflow.exceptions import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.ydb.hooks._vendor.dbapi.connection import Connection as DbApiConnection
from airflow.providers.ydb.hooks._vendor.dbapi.cursor import YdbQuery
from airflow.providers.ydb.utils.credentials import get_credentials_from_connection
from airflow.providers.ydb.utils.defaults import CONN_NAME_ATTR, CONN_TYPE, DEFAULT_CONN_NAME

DEFAULT_YDB_GRPCS_PORT: int = 2135

if TYPE_CHECKING:
    from airflow.models.connection import Connection
    from airflow.providers.ydb.hooks._vendor.dbapi.cursor import Cursor as DbApiCursor


class YDBCursor:
    """YDB cursor wrapper."""

    def __init__(self, delegatee: DbApiCursor, is_ddl: bool):
        self.delegatee: DbApiCursor = delegatee
        self.is_ddl: bool = is_ddl

    def execute(self, sql: str, parameters: Mapping[str, Any] | None = None):
        if parameters is not None:
            raise AirflowException("parameters is not supported yet")

        q = YdbQuery(yql_text=sql, is_ddl=self.is_ddl)
        return self.delegatee.execute(q, parameters)

    def executemany(self, sql: str, seq_of_parameters: Sequence[Mapping[str, Any]]):
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
        return self.delegatee.setinputsizes(sizes)

    def setoutputsize(self, column=None):
        return self.delegatee.setoutputsize(column)

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
    """YDB connection wrapper."""

    def __init__(self, ydb_session_pool: Any, is_ddl: bool, use_scan_query: bool):
        self.is_ddl = is_ddl
        self.use_scan_query = use_scan_query
        self.delegatee: DbApiConnection = DbApiConnection(ydb_session_pool=ydb_session_pool)
        self.delegatee.set_ydb_scan_query(use_scan_query)

    def cursor(self) -> YDBCursor:
        return YDBCursor(self.delegatee.cursor(), is_ddl=self.is_ddl)

    def begin(self) -> None:
        self.delegatee.begin()

    def commit(self) -> None:
        self.delegatee.commit()

    def rollback(self) -> None:
        self.delegatee.rollback()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        self.delegatee.close()

    def bulk_upsert(self, table_name: str, rows: Sequence, column_types: ydb.BulkUpsertColumns):
        self.delegatee.driver.table_client.bulk_upsert(table_name, rows=rows, column_types=column_types)


class YDBHook(DbApiHook):
    """Interact with YDB."""

    conn_name_attr: str = CONN_NAME_ATTR
    default_conn_name: str = DEFAULT_CONN_NAME
    conn_type: str = CONN_TYPE
    hook_name: str = "YDB"
    supports_autocommit: bool = True
    supports_executemany: bool = True

    def __init__(self, *args, is_ddl: bool = False, use_scan_query: bool = False, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.is_ddl = is_ddl
        self.use_scan_query = use_scan_query

        conn: Connection = self.get_connection(self.get_conn_id())
        host: str | None = conn.host
        if not host:
            raise ValueError("YDB host must be specified")
        port: int = conn.port or DEFAULT_YDB_GRPCS_PORT

        connection_extra: dict[str, Any] = conn.extra_dejson
        database: str | None = connection_extra.get("database")
        if not database:
            raise ValueError("YDB database must be specified")
        self.database: str = database

        endpoint = f"{host}:{port}"
        credentials = get_credentials_from_connection(
            endpoint=endpoint, database=database, connection=conn, connection_extra=connection_extra
        )

        driver_config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            table_client_settings=YDBHook._get_table_client_settings(),
            credentials=credentials,
        )
        driver = ydb.Driver(driver_config)
        # wait until driver become initialized
        driver.wait(fail_fast=True, timeout=10)
        self.ydb_session_pool = ydb.SessionPool(driver, size=5)

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to YDB connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, PasswordField, StringField

        return {
            "database": StringField(
                lazy_gettext("Database name"),
                widget=BS3TextFieldWidget(),
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
                description="User account IAM token.",
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
                "database": "e.g. /local or /ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b",
                "service_account_json": 'e.g. {"id": "...", "service_account_id": "...", "private_key": "..."}',
                "token": "t1.9....AAQ",
            },
        }

    @property
    def sqlalchemy_url(self) -> URL:
        conn: Connection = self.get_connection(self.get_conn_id())
        return URL.create(
            drivername="ydb",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            query={"database": self.database},
        )

    def get_conn(self) -> YDBConnection:
        """Establish a connection to a YDB database."""
        return YDBConnection(self.ydb_session_pool, is_ddl=self.is_ddl, use_scan_query=self.use_scan_query)

    @staticmethod
    def _serialize_cell(cell: object, conn: YDBConnection | None = None) -> Any:
        return cell

    def bulk_upsert(self, table_name: str, rows: Sequence, column_types: ydb.BulkUpsertColumns):
        """
        BulkUpsert into database. More optimal way to insert rows into db.

        .. seealso::

            https://ydb.tech/docs/en/recipes/ydb-sdk/bulk-upsert
        """
        self.get_conn().bulk_upsert(f"{self.database}/{table_name}", rows, column_types)

    @staticmethod
    def _get_table_client_settings() -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )
