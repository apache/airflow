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

import warnings
from collections import namedtuple
from contextlib import closing
from copy import copy
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Sequence,
    TypeVar,
    cast,
    overload,
)

from databricks import sql  # type: ignore[attr-defined]

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook, return_single_query_results

LIST_SQL_ENDPOINTS_ENDPOINT = ("GET", "api/2.0/sql/endpoints")


T = TypeVar("T")


class TypedefHook(DbApiHook):

    hook_name = "Typedef"
    _test_connection_sql = "select 42"

    def __init__(
        self,
        typedef_conn_id: str = "",
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration: dict[str, str] | None = None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        caller: str = "DatabricksSqlHook",
        return_tuple: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(typedef_conn_id, caller=caller)
        self._sql_conn = None
        self._token: str | None = None
        self._http_path = http_path
        self._sql_endpoint_name = sql_endpoint_name
        self.supports_autocommit = True
        self.session_config = session_configuration
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema
        self.return_tuple = return_tuple
        self.additional_params = kwargs

    def get_conn(self) -> Connection:
        if not self._http_path:
            if self._sql_endpoint_name:
                endpoint = self._get_sql_endpoint_by_name(self._sql_endpoint_name)
                self._http_path = endpoint["odbc_params"]["path"]
            elif "http_path" in self.databricks_conn.extra_dejson:
                self._http_path = self.databricks_conn.extra_dejson["http_path"]
            else:
                raise AirflowException()

        requires_init = True
        if not self._token:
            self._token = self._get_token(raise_error=True)
        else:
            new_token = self._get_token(raise_error=True)
            if new_token != self._token:
                self._token = new_token
            else:
                requires_init = False

        if not self.session_config:
            self.session_config = self.databricks_conn.extra_dejson.get("session_configuration")

        if not self._sql_conn or requires_init:
            if self._sql_conn:  # close already existing connection
                self._sql_conn.close()
            self._sql_conn = sql.connect(
                self.host,
                self._http_path,
                self._token,
                schema=self.schema,
                catalog=self.catalog,
                session_configuration=self.session_config,
                http_headers=self.http_headers,
                _user_agent_entry=self.user_agent_value,
                **self._get_extra_config(),
                **self.additional_params,
            )
        return self._sql_conn

    @overload  # type: ignore[override]
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

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = True,
        return_last: bool = True,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None: ...
