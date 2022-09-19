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

from typing import TYPE_CHECKING, Iterable, Mapping, Sequence

from psycopg2.sql import SQL, Identifier

from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.www import utils as wwwutils

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PostgresOperator(BaseOperator):
    """
    Executes sql code in a specific Postgres database

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
    """

    template_fields: Sequence[str] = ('sql',)
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {
        'sql': 'postgresql' if 'postgresql' in wwwutils.get_attr_renderer() else 'sql'
    }
    template_ext: Sequence[str] = ('.sql',)
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: str | Iterable[str],
        postgres_conn_id: str = 'postgres_default',
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        database: str | None = None,
        runtime_parameters: Mapping | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.runtime_parameters = runtime_parameters
        self.hook: PostgresHook | None = None

    def execute(self, context: Context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, database=self.database)
        if self.runtime_parameters:
            final_sql = []
            sql_param = {}
            for param in self.runtime_parameters:
                set_param_sql = f"SET {{}} TO %({param})s;"
                dynamic_sql = SQL(set_param_sql).format(Identifier(f"{param}"))
                final_sql.append(dynamic_sql)
            for param, val in self.runtime_parameters.items():
                sql_param.update({f"{param}": f"{val}"})
            if self.parameters:
                sql_param.update(self.parameters)
            if isinstance(self.sql, str):
                final_sql.append(SQL(self.sql))
            else:
                final_sql.extend(list(map(SQL, self.sql)))
            self.hook.run(final_sql, self.autocommit, parameters=sql_param)
        else:
            self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
