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
from typing import Iterable, Mapping, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.utils.decorators import apply_defaults


class MsSqlOperator(BaseOperator):
    """
    Executes sql code in a specific Microsoft SQL database

    This operator may use one of two hooks, depending on the ``conn_type`` of the connection.

    If conn_type is ``'odbc'``, then :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`
    is used.  Otherwise, :py:class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook` is used.

    :param sql: the sql code to be executed
    :type sql: str or string pointing to a template file with .sql
        extension. (templated)
    :param mssql_conn_id: reference to a specific mssql database
    :type mssql_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ('sql',)
    template_ext = ('.sql',)
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
        self, *,
        sql: str,
        mssql_conn_id: str = 'mssql_default',
        parameters: Optional[Union[Mapping, Iterable]] = None,
        autocommit: bool = False,
        database: Optional[str] = None,
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.mssql_conn_id = mssql_conn_id
        self.sql = sql
        self.parameters = parameters
        self.autocommit = autocommit
        self.database = database
        self._hook = None

    def get_hook(self):
        """
        Will retrieve hook as determined by Connection.

        If conn_type is ``'odbc'``, will use
        :py:class:`~airflow.providers.odbc.hooks.odbc.OdbcHook`.
        Otherwise, :py:class:`~airflow.providers.microsoft.mssql.hooks.mssql.MsSqlHook` will be used.
        """
        if not self._hook:
            conn = MsSqlHook.get_connection(conn_id=self.mssql_conn_id)
            try:
                self._hook: Union[MsSqlHook, OdbcHook] = conn.get_hook()
                self._hook.schema = self.database
            except AirflowException:
                self._hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        return self._hook

    def execute(self, context):
        self.log.info('Executing: %s', self.sql)
        hook = self.get_hook()
        hook.run(sql=self.sql, autocommit=self.autocommit, parameters=self.parameters)
