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
from airflow.hooks.base import BaseHook as BaseForDbApiHook
from typing import Any, Callable, Iterable, Mapping, Sequence, Union
from typing_extensions import Protocol

def return_single_query_results(
    sql: Union[str, Iterable[str]], return_last: bool, split_statements: bool
): ...
def fetch_all_handler(cursor) -> Union[list[tuple], None]: ...
def fetch_one_handler(cursor) -> Union[list[tuple], None]: ...

class ConnectorProtocol(Protocol):
    def connect(self, host: str, port: int, username: str, schema: str) -> Any: ...

class DbApiHook(BaseForDbApiHook):
    conn_name_attr: str
    default_conn_name: str
    supports_autocommit: bool
    connector: Union[ConnectorProtocol, None]
    placeholder: str
    log_sql: Incomplete
    descriptions: Incomplete
    _placeholder: str
    def __init__(self, *args, schema: Union[str, None] = ..., log_sql: bool = ..., **kwargs) -> None: ...
    def get_conn(self): ...
    def get_uri(self) -> str: ...
    def get_sqlalchemy_engine(self, engine_kwargs: Incomplete | None = ...): ...
    def get_pandas_df(self, sql, parameters: Incomplete | None = ..., **kwargs): ...
    def get_pandas_df_by_chunks(
        self, sql, parameters: Incomplete | None = ..., *, chunksize, **kwargs
    ) -> None: ...
    def get_records(
        self, sql: Union[str, list[str]], parameters: Union[Iterable, Mapping, None] = ...
    ) -> Any: ...
    def get_first(
        self, sql: Union[str, list[str]], parameters: Union[Iterable, Mapping, None] = ...
    ) -> Any: ...
    @staticmethod
    def strip_sql_string(sql: str) -> str: ...
    @staticmethod
    def split_sql_string(sql: str) -> list[str]: ...
    @property
    def last_description(self) -> Union[Sequence[Sequence], None]: ...
    def run(
        self,
        sql: Union[str, Iterable[str]],
        autocommit: bool = ...,
        parameters: Union[Iterable, Mapping, None] = ...,
        handler: Union[Callable, None] = ...,
        split_statements: bool = ...,
        return_last: bool = ...,
    ) -> Union[Any, list[Any], None]: ...
    def set_autocommit(self, conn, autocommit) -> None: ...
    def get_autocommit(self, conn) -> bool: ...
    def get_cursor(self): ...
    def insert_rows(
        self,
        table,
        rows,
        target_fields: Incomplete | None = ...,
        commit_every: int = ...,
        replace: bool = ...,
        **kwargs,
    ) -> None: ...
    def bulk_dump(self, table, tmp_file) -> None: ...
    def bulk_load(self, table, tmp_file) -> None: ...
    def test_connection(self): ...
