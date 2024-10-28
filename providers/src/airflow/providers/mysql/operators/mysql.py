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

from typing import Sequence

from deprecated import deprecated

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@deprecated(
    reason=(
        "Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`."
        "Also, you can provide `hook_params={'schema': <database>}`."
    ),
    category=AirflowProviderDeprecationWarning,
)
class MySqlOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific MySQL database.

    This class is deprecated.

    Please use :class:`airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:mysql`

    :param sql: the sql code to be executed. Can receive a str representing a
        sql statement, a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    :param parameters: (optional) the parameters to render the SQL query with.
        Template reference are recognized by str ending in '.json'
        (templated)
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :param database: name of database which overwrite defined one in connection
    """

    template_fields: Sequence[str] = ("sql", "parameters")
    template_fields_renderers = {
        "sql": "mysql",
        "parameters": "json",
    }
    template_ext: Sequence[str] = (".sql", ".json")
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        mysql_conn_id: str = "mysql_default",
        database: str | None = None,
        **kwargs,
    ) -> None:
        if database is not None:
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {"schema": database, **hook_params}

        super().__init__(conn_id=mysql_conn_id, **kwargs)
