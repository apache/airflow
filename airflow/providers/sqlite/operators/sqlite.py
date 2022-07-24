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
from typing import Any, Iterable, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.sqlite.hooks.sqlite import SqliteHook


class SqliteOperator(BaseOperator):
    """
    Executes sql code in a specific Sqlite database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SqliteOperator`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :param sqlite_conn_id: reference to a specific sqlite database
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#cdaaed'

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        sqlite_conn_id: str = 'sqlite_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.sqlite_conn_id = sqlite_conn_id
        self.sql = sql
        self.parameters = parameters or []

    def execute(self, context: Mapping[Any, Any]) -> None:
        self.log.info('Executing: %s', self.sql)
        hook = SqliteHook(sqlite_conn_id=self.sqlite_conn_id)
        hook.run(self.sql, parameters=self.parameters)
