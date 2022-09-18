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

from airflow.models import BaseOperator
from airflow.providers.exasol.hooks.exasol import ExasolHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class ExasolOperator(BaseOperator):
    """
    Executes sql code in a specific Exasol database

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        template references are recognized by str ending in '.sql'
    :param exasol_conn_id: reference to a specific Exasol database
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    :param schema: (optional) name of the schema which overwrite defined one in connection
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: str | Iterable[str],
        exasol_conn_id: str = 'exasol_default',
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        schema: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.exasol_conn_id = exasol_conn_id
        self.sql = sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.schema = schema

    def execute(self, context: Context) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = ExasolHook(exasol_conn_id=self.exasol_conn_id, schema=self.schema)
        hook.run(self.sql, autocommit=self.autocommit, parameters=self.parameters)
