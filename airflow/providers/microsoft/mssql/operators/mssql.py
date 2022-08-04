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
from typing import TYPE_CHECKING, Iterable, Mapping, Optional, Sequence, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.www import utils as wwwutils

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.utils.context import Context


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MsSqlOperator`

    This operator may use one of two hooks, depending on the ``conn_type`` of the connection.

    If conn_type is ``'odbc'``, then :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`
    is used.  Otherwise, :py:class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook` is used.

    :param sql: the sql code to be executed (templated)
    :param mssql_conn_id: reference to a specific mssql database
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param database: name of database which overwrite defined one in connection
    """

    template_fields: Sequence[str] = ('sql',)
    template_ext: Sequence[str] = ('.sql',)
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {'sql': 'tsql' if 'tsql' in wwwutils.get_attr_renderer() else 'sql'}
    ui_color = '#ededed'

    def __init__(
        self,
        *,
        sql: Union[str, Iterable[str]],
        mssql_conn_id: str = 'mssql_default',
        parameters: Optional[Union[Iterable, Mapping]] = None,
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self._hook: Optional[Union[MsSqlHook, 'DbApiHook']] = None

    def get_hook(self) -> Optional[Union[MsSqlHook, 'DbApiHook']]:
        """
        Will retrieve hook as determined by :meth:`~.Connection.get_hook` if one is defined, and
        :class:`~.MsSqlHook` otherwise.

        For example, if the connection ``conn_type`` is ``'odbc'``, :class:`~.OdbcHook` will be used.
        """
        if not self._hook:
            conn = MsSqlHook.get_connection(conn_id=self.mssql_conn_id)
            try:
                self._hook = conn.get_hook()
                self._hook.schema = self.database  # type: ignore[union-attr]
            except AirflowException:
                self._hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        return self._hook

    def execute(self, context: 'Context') -> None:
        self.log.info('Executing: %s', self.sql)
        hook = self.get_hook()
        hook.run(  # type: ignore[union-attr]
            sql=self.sql, autocommit=self.autocommit, parameters=self.parameters
        )
