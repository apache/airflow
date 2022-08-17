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

from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import fetch_all_handler
from airflow.providers.jdbc.hooks.jdbc import JdbcHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class JdbcOperator(BaseOperator):
    """
    Executes sql code in a database using jdbc driver.

    Requires jaydebeapi.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:JdbcOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param jdbc_conn_id: reference to a predefined database
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param parameters: (optional) the parameters to render the SQL query with.
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    template_fields_renderers = {'sql': 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        jdbc_conn_id: str = 'jdbc_default',
        autocommit: bool = False,
        parameters: Optional[Union[Iterable, Mapping]] = None,
        handler: Callable[[Any], Any] = fetch_all_handler,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.parameters = parameters
        self.sql = sql
        self.jdbc_conn_id = jdbc_conn_id
        self.autocommit = autocommit
        self.handler = handler
        self.hook = None

    def execute(self, context: 'Context'):
        self.log.info('Executing: %s', self.sql)
        hook = JdbcHook(jdbc_conn_id=self.jdbc_conn_id)
        if self.do_xcom_push:
            return hook.run(self.sql, self.autocommit, parameters=self.parameters, handler=self.handler)
        else:
            return hook.run(self.sql, self.autocommit, parameters=self.parameters)
