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
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping, Sequence

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.helpers import exactly_one

if TYPE_CHECKING:
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
    """
    Operator for ingesting text and embeddings into a PostgreSQL database using the pgvector library.

    :param conn_id: The connection ID for the postgresql database.
    :param input_data: Tuple containing the string input content and corresponding list of float vector
        embeddings.
    :param input_callable: A callable that returns a tuple containing the string input content and
        corresponding  list of float vector embeddings, if ``input_data`` is not provided.
    :param input_callable_args: Positional arguments for the 'input_callable'.
    :param input_callable_kwargs: Keyword arguments for the 'input_callable'.
    :param kwargs: Additional keyword arguments for the BaseOperator.
    """

    def __init__(
        self,
        conn_id: str,
        input_data: tuple[str, list[float]] | None = None,
        input_callable: Callable[[Any], Any] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.conn_id = conn_id
        self.input_data = input_data
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args
        self.input_callable_kwargs = input_callable_kwargs
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        """
        Executes the ingestion process.

        This method either uses the provided embedding or computes the embedding using the
        provided callable. The text and its associated embedding are then stored into the
        PostgreSQL database's 'documents' table.

        :param context: The execution context for the operator.
        """
        from pgvector.psycopg import register_vector

        if not exactly_one(self.input_data, self.input_callable):
            raise ValueError("Either `input_data` or `input_callable` must be provided, not both.")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            input_data = self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        else:
            input_data = self.input_data
        conn = PostgresHook(postgres_conn_id=self.conn_id).conn
        register_vector(conn)
        content = input_data[0]
        embeddings = input_data[1]
        conn.execute("INSERT INTO documents (content, embedding) VALUES (%s, %s)", (content, embeddings))
