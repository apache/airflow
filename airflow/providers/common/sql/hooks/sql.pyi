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
# This is automatically generated stub for the `common.sql` provider
#
# This file is generated automatically by the `update-common-sql-api stubs` pre-commit
# and the .pyi file represents part of the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""
Definition of the public interface for airflow.providers.common.sql.hooks.sql
isort:skip_file
"""
from _typeshed import Incomplete
from airflow.exceptions import (
    AirflowException as AirflowException,
    AirflowOptionalProviderFeatureException as AirflowOptionalProviderFeatureException,
    AirflowProviderDeprecationWarning as AirflowProviderDeprecationWarning,
)
from airflow.hooks.base import BaseHook as BaseHook
from airflow.providers.openlineage.extractors import OperatorLineage as OperatorLineage
from airflow.providers.openlineage.sqlparser import DatabaseInfo as DatabaseInfo
from pandas import DataFrame as DataFrame
from typing import Any, Callable, Generator, Iterable, Mapping, Protocol, Sequence, TypeVar, overload

T = TypeVar("T")
SQL_PLACEHOLDERS: Incomplete

def return_single_query_results(sql: str | Iterable[str], return_last: bool, split_statements: bool): ...
def fetch_all_handler(cursor) -> list[tuple] | None: ...
def fetch_one_handler(cursor) -> list[tuple] | None: ...

class ConnectorProtocol(Protocol):
    def connect(self, host: str, port: int, username: str, schema: str) -> Any: ...

class DbApiHook(BaseHook):
    conn_name_attr: str
    default_conn_name: str
    supports_autocommit: bool
    connector: ConnectorProtocol | None
    log_sql: Incomplete
    descriptions: Incomplete
    def __init__(self, *args, schema: str | None = None, log_sql: bool = True, **kwargs) -> None: ...
    @property
    def placeholder(self): ...
    def get_conn(self): ...
    def get_uri(self) -> str: ...
    def get_sqlalchemy_engine(self, engine_kwargs: Incomplete | None = None): ...
    def get_pandas_df(
        self, sql, parameters: list | tuple | Mapping[str, Any] | None = None, **kwargs
    ) -> DataFrame: ...
    def get_pandas_df_by_chunks(
        self, sql, parameters: list | tuple | Mapping[str, Any] | None = None, *, chunksize: int, **kwargs
    ) -> Generator[DataFrame, None, None]: ...
    def get_records(
        self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any: ...
    def get_first(
        self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any: ...
    @staticmethod
    def strip_sql_string(sql: str) -> str: ...
    @staticmethod
    def split_sql_string(sql: str) -> list[str]: ...
    @property
    def last_description(self) -> Sequence[Sequence] | None: ...
    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: None = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> None: ...
    @overload
    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = ...,
        parameters: Iterable | Mapping[str, Any] | None = ...,
        handler: Callable[[Any], T] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...
    def set_autocommit(self, conn, autocommit) -> None: ...
    def get_autocommit(self, conn) -> bool: ...
    def get_cursor(self): ...
    def insert_rows(
        self,
        table,
        rows,
        target_fields: Incomplete | None = None,
        commit_every: int = 1000,
        replace: bool = False,
        *,
        executemany: bool = False,
        **kwargs,
    ): ...
    def bulk_dump(self, table, tmp_file) -> None: ...
    def bulk_load(self, table, tmp_file) -> None: ...
    def test_connection(self): ...
    def get_openlineage_database_info(self, connection) -> DatabaseInfo | None: ...
    def get_openlineage_database_dialect(self, connection) -> str: ...
    def get_openlineage_default_schema(self) -> str | None: ...
    def get_openlineage_database_specific_lineage(self, task_instance) -> OperatorLineage | None: ...
    @staticmethod
    def get_openlineage_authority_part(connection, default_port: int | None = None) -> str: ...
