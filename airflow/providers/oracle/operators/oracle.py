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

import re
from typing import TYPE_CHECKING, Sequence

import oracledb
from deprecated import deprecated

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


@deprecated(
    reason="Please use `airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.",
    category=AirflowProviderDeprecationWarning,
)
class OracleOperator(SQLExecuteQueryOperator):
    """
    Executes sql code in a specific Oracle database.

    This class is deprecated.

    Please use :class:`airflow.providers.common.sql.operators.sql.SQLExecuteQueryOperator`.

    :param sql: the sql code to be executed. Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
        (templated)
    :param oracle_conn_id: The :ref:`Oracle connection id <howto/connection:oracle>`
        reference to a specific Oracle database.
    :param parameters: (optional, templated) the parameters to render the SQL query with.
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    """

    template_fields: Sequence[str] = (
        "parameters",
        "sql",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#ededed"

    def __init__(self, *, oracle_conn_id: str = "oracle_default", **kwargs) -> None:
        super().__init__(conn_id=oracle_conn_id, **kwargs)


class OracleStoredProcedureOperator(BaseOperator):
    """
    Executes stored procedure in a specific Oracle database.

    :param procedure: name of stored procedure to call (templated)
    :param oracle_conn_id: The :ref:`Oracle connection id <howto/connection:oracle>`
        reference to a specific Oracle database.
    :param parameters: (optional, templated) the parameters provided in the call

    If *do_xcom_push* is *True*, the numeric exit code emitted by
    the database is pushed to XCom under key ``ORA`` in case of failure.
    """

    template_fields: Sequence[str] = (
        "parameters",
        "procedure",
    )
    ui_color = "#ededed"

    def __init__(
        self,
        *,
        procedure: str,
        oracle_conn_id: str = "oracle_default",
        parameters: dict | list | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.procedure = procedure
        self.parameters = parameters

    def execute(self, context: Context):
        self.log.info("Executing: %s", self.procedure)
        hook = OracleHook(oracle_conn_id=self.oracle_conn_id)
        try:
            return hook.callproc(self.procedure, autocommit=True, parameters=self.parameters)
        except oracledb.DatabaseError as e:
            if not self.do_xcom_push or not context:
                raise
            ti = context["ti"]
            code_match = re.search("^ORA-(\\d+):.+", str(e))
            if code_match:
                ti.xcom_push(key="ORA", value=code_match.group(1))
            raise
