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
"""This module is deprecated. Please use `airflow.providers.sqlite.operators.sqlite`."""

import warnings

# pylint: disable=unused-import
from airflow.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.base_sensor_operator import apply_defaults

warnings.warn(
    "This module is deprecated. Please use `airflow.operators.sql`.",
    DeprecationWarning, stacklevel=2
)


class SqliteOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Sqlite database

    .. warning::

        This class is deprecated.
        Please use :class:`airflow.operators.sql.SQLExecuteQueryOperator`.

    :param sql: the sql code to be executed. (templated)
    :type sql: str or string pointing to a template file. File must have
        a '.sql' extensions.
    :param sqlite_conn_id: reference to a specific sqlite database
    :type sqlite_conn_id: str
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    """

    @apply_defaults
    def __init__(
        self,
        *,
        sqlite_conn_id: str = 'sqlite_default',
        **kwargs
    ) -> None:
        super().__init__(conn_id=sqlite_conn_id, **kwargs)
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.operators.sql.SQLExecuteQueryOperator`.""",
            DeprecationWarning, stacklevel=2
        )
