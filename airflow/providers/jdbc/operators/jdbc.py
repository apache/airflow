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


class JdbcOperator(SQLExecuteQueryOperator):
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

    def __init__(self, *, jdbc_conn_id: str = 'jdbc_default', **kwargs) -> None:
        super().__init__(conn_id=jdbc_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
