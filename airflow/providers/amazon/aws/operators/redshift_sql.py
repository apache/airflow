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


class RedshiftSQLOperator(SQLExecuteQueryOperator):
    """
    Executes SQL Statements against an Amazon Redshift cluster

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:RedshiftSQLOperator`

    :param sql: the SQL code to be executed as a single string, or
        a list of str (sql statements), or a reference to a template file.
        Template references are recognized by str ending in '.sql'
    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    template_fields: Sequence[str] = (
        'sql',
        'redshift_conn_id',
    )
    template_ext: Sequence[str] = ('.sql',)
    # TODO: Remove renderer check when the provider has an Airflow 2.3+ requirement.
    template_fields_renderers = {
        "sql": "postgresql" if "postgresql" in wwwutils.get_attr_renderer() else "sql"
    }

    def __init__(self, *, redshift_conn_id: str = 'redshift_default', **kwargs) -> None:
        super().__init__(conn_id=redshift_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning,
            stacklevel=2,
        )
