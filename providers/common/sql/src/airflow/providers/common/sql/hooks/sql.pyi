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
# This file is generated automatically by the `update-common-sql-api stubs` prek hook
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
Definition of the public interface for
airflow.providers.common.sql.src.airflow.providers.common.sql.hooks.sql.
"""

from collections.abc import Callable, Generator, Iterable, Mapping, MutableMapping, Sequence
from functools import cached_property as cached_property
from typing import Any, Literal, Protocol, TypeVar, overload

from _typeshed import Incomplete as Incomplete
from pandas import DataFrame as PandasDataFrame
from polars import DataFrame as PolarsDataFrame
from sqlalchemy.engine import URL as URL, Engine as Engine, Inspector as Inspector

from airflow.hooks.base import BaseHook as BaseHook
from airflow.models import Connection as Connection
from airflow.providers.common.sql.dialects.dialect import Dialect as Dialect
from airflow.providers.openlineage.extractors import OperatorLineage as OperatorLineage
from airflow.providers.openlineage.sqlparser import DatabaseInfo as DatabaseInfo

T = TypeVar("T")
SQL_PLACEHOLDERS: Incomplete
WARNING_MESSAGE: str

def return_single_query_results(
    sql: str | Iterable[str], return_last: bool, split_statements: bool | None
): ...
def fetch_all_handler(cursor) -> list[tuple] | None: ...
def fetch_one_handler(cursor) -> list[tuple] | None: ...
def resolve_dialects() -> MutableMapping[str, MutableMapping]: ...

class ConnectorProtocol(Protocol):
    def connect(self, host: str, port: int, username: str, schema: str) -> Any: ...

class DbApiHook(BaseHook):
    conn_name_attr: str
    default_conn_name: str
    strip_semicolon: bool
    supports_autocommit: bool
    supports_executemany: bool
    connector: ConnectorProtocol | None
    log_sql: Incomplete
    descriptions: Incomplete
    def __init__(self, *args, schema: str | None = None, log_sql: bool = True, **kwargs) -> None: ...
    def get_conn_id(self) -> str: ...
    @cached_property
    def placeholder(self) -> str: ...
    @property
    def insert_statement_format(self) -> str: ...
    @property
    def replace_statement_format(self) -> str: ...
    @property
    def escape_word_format(self) -> str: ...
    @property
    def escape_column_names(self) -> bool: ...
    @property
    def connection(self) -> Connection: ...
    @connection.setter
    def connection(self, value: Any) -> None: ...
    @property
    def connection_extra(self) -> dict: ...
    @cached_property
    def connection_extra_lower(self) -> dict: ...
    def get_conn(self) -> Any: ...
    def get_uri(self) -> str: ...
    @property
    def sqlalchemy_url(self) -> URL: ...
    def get_sqlalchemy_engine(self, engine_kwargs: Incomplete | None = None) -> Engine: ...
    @property
    def inspector(self) -> Inspector: ...
    @cached_property
    def dialect_name(self) -> str: ...
    @cached_property
    def dialect(self) -> Dialect: ...
    @property
    def reserved_words(self) -> set[str]: ...
    def get_reserved_words(self, dialect_name: str) -> set[str]: ...
    def get_pandas_df(
        self, sql, parameters: list | tuple | Mapping[str, Any] | None = None, **kwargs
    ) -> PandasDataFrame: ...
    def get_pandas_df_by_chunks(
        self, sql, parameters: list | tuple | Mapping[str, Any] | None = None, *, chunksize: int, **kwargs
    ) -> Generator[PandasDataFrame, None, None]: ...
    def get_records(
        self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any: ...
    def get_first(
        self, sql: str | list[str], parameters: Iterable | Mapping[str, Any] | None = None
    ) -> Any: ...
    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas"] = "pandas",
        **kwargs: Any,
    ) -> PandasDataFrame: ...
    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["polars"],
        **kwargs: Any,
    ) -> PolarsDataFrame: ...
    @overload
    def get_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        df_type: Literal["pandas"] = "pandas",
        **kwargs,
    ) -> Generator[PandasDataFrame, None, None]: ...
    @overload
    def get_df_by_chunks(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        chunksize: int,
        df_type: Literal["polars"],
        **kwargs,
    ) -> Generator[PolarsDataFrame, None, None]: ...
    @staticmethod
    def strip_sql_string(sql: str) -> str: ...
    @staticmethod
    def split_sql_string(sql: str, strip_semicolon: bool = False) -> list[str]: ...
    @property
    def last_description(self) -> Sequence[Sequence] | None: ...
    def set_autocommit(self, conn, autocommit) -> None: ...
    def get_autocommit(self, conn) -> bool: ...
    def get_cursor(self) -> Any: ...
    def insert_rows(
        self,
        table,
        rows,
        target_fields: Incomplete | None = None,
        commit_every: int = 1000,
        replace: bool = False,
        *,
        executemany: bool = False,
        autocommit: bool = False,
        **kwargs,
    ): ...
    def bulk_dump(self, table, tmp_file) -> None: ...
    def bulk_load(self, table, tmp_file) -> None: ...
    def test_connection(self) -> None: ...
    def get_openlineage_database_info(self, connection) -> DatabaseInfo | None: ...
    def get_openlineage_database_dialect(self, connection) -> str: ...
    def get_openlineage_default_schema(self) -> str | None: ...
    def get_openlineage_database_specific_lineage(self, task_instance) -> OperatorLineage | None: ...
    @staticmethod
    def get_openlineage_authority_part(connection, default_port: int | None = None) -> str: ...
    def get_db_log_messages(self, conn) -> None: ...
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
    ) -> tuple | list | list[tuple] | list[list[tuple] | tuple] | None: ...
