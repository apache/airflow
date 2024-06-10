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

from __future__ import annotations

import csv
import json
from typing import TYPE_CHECKING, Any, Sequence


from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.typedef.hooks.typedef_hook import TypedefHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TypedefOperator(SQLExecuteQueryOperator):

    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    conn_id_field = "typedef_conn_id"

    template_fields: Sequence[str] = tuple(
        {"_output_path", "schema", "catalog", "http_headers", f"{conn_id_field}"}
        | set(SQLExecuteQueryOperator.template_fields)
    )

    def __init__(
        self,
        *,
        typedef_conn_id: str = "",
        http_path: str | None = None,
        sql_endpoint_name: str | None = None,
        session_configuration=None,
        http_headers: list[tuple[str, str]] | None = None,
        catalog: str | None = None,
        schema: str | None = None,
        output_path: str | None = None,
        output_format: str = "csv",
        csv_params: dict[str, Any] | None = None,
        client_parameters: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(conn_id=typedef_conn_id, **kwargs)
        self.typedef_conn_id = typedef_conn_id
        self._output_path = output_path
        self._output_format = output_format
        self._csv_params = csv_params
        self.http_path = http_path
        self.sql_endpoint_name = sql_endpoint_name
        self.session_configuration = session_configuration
        self.client_parameters = {} if client_parameters is None else client_parameters
        self.hook_params = kwargs.pop("hook_params", {})
        self.http_headers = http_headers
        self.catalog = catalog
        self.schema = schema

    def get_db_hook(self) -> TypedefHook:
        hook_params = {
            "http_path": self.http_path,
            "session_configuration": self.session_configuration,
            "sql_endpoint_name": self.sql_endpoint_name,
            "http_headers": self.http_headers,
            "catalog": self.catalog,
            "schema": self.schema,
            "caller": "TypedefOperator",
            "return_tuple": True,
            **self.client_parameters,
            **self.hook_params,
        }
        return TypedefHook(**hook_params)
