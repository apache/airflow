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
from typing import Sequence

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.www import utils as wwwutils


class MsSqlOperator(SQLExecuteQueryOperator):
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
        self, *, mssql_conn_id: str = 'mssql_default', database: str | None = None, **kwargs
    ) -> None:
        if database is not None:
            hook_params = kwargs.pop('hook_params', {})
            kwargs['hook_params'] = {'schema': database, **hook_params}

        super().__init__(conn_id=mssql_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.
            Also, you can provide `hook_params={'schema': <database>}`.""",
            DeprecationWarning,
            stacklevel=2,
        )
