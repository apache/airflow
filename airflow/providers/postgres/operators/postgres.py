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

import warnings
from typing import Mapping, Sequence, Callable, Any, Collection

from airflow.exceptions import AirflowProviderDeprecationWarning, AirflowException
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context


class PostgresOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Postgres database.

    This class is deprecated.

    Please use :class:`airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param database: name of database which overwrite defined one in connection
    :param runtime_parameters: a mapping of runtime params added to the final sql being executed.
        For example, you could set the schema via `{"search_path": "CUSTOM_SCHEMA"}`.
        Deprecated - use `hook_params={'options': '-c <connection_options>'}` instead.
    """

    template_fields: Sequence[str] = ("sql",)
    template_fields_renderers = {"sql": "postgresql"}
    template_ext: Sequence[str] = (".sql",)
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        postgres_conn_id: str = "postgres_default",
        database: str | None = None,
        runtime_parameters: Mapping | None = None,
        **kwargs,
    ) -> None:
        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"schema": database, **hook_params}

        if runtime_parameters:
            warnings.warn(
                """`runtime_parameters` is deprecated.
                Please use `hook_params={'options': '-c <connection_options>}`.""",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            hook_params = kwargs.pop("hook_params", {})
            options = " ".join(f"-c {param}={val}" for param, val in runtime_parameters.items())
            kwargs["hook_params"] = {"options": options, **hook_params}

        super().__init__(conn_id=postgres_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.
            Also, you can provide `hook_params={'schema': <database>}`.""",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )


class PgVectorIngestOperator(BaseOperator):

    def __init__(
        self,
        conn_id: str,
        input_text: str | None = None,
        input_embedding: list[float] | None = None,
        input_callable: Callable[[Any], Any] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.conn_id = conn_id
        self.input_text = input_text
        self.input_embedding = input_embedding
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args
        self.input_callable_kwargs = input_callable_kwargs
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        from pgvector.psycopg import register_vector

        if all([self.input_embedding, self.input_callable]):
            raise AirflowException("Only one of 'input_embedding' and 'input_callable' is allowed")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            input_embedding = self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        else:
            input_embedding = self.input_embedding
        conn = PostgresHook(postgres_conn_id=self.conn_id).conn
        register_vector(conn)
        conn.execute(
            'INSERT INTO documents (content, embedding) VALUES (%s, %s)',
            (self.input_text, input_embedding)
        )
