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

from typing import Any, Callable, Collection, Mapping, Sequence

from airflow.providers.common.compat.standard.operators import PythonOperator, get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils.snowpark import inject_session_into_op_kwargs


class SnowparkOperator(PythonOperator):
    """
    Executes a Python function with Snowpark Python code.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SnowparkOperator`

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param python_callable: A reference to an object that is callable
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied. (templated)
    :param templates_exts: a list of file extensions to resolve while
        processing templated fields, for examples ``['.sql', '.hql']``
    :param show_return_value_in_logs: a bool value whether to show return_value
        logs. Defaults to True, which allows return value log output.
        It can be set to False to prevent log output of return value when you return huge data
        such as transmission a large amount of XCom to TaskAPI.
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at
        the time you connect to Snowflake
    """

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        templates_dict: dict[str, Any] | None = None,
        templates_exts: Sequence[str] | None = None,
        show_return_value_in_logs: bool = True,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ):
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            templates_dict=templates_dict,
            templates_exts=templates_exts,
            show_return_value_in_logs=show_return_value_in_logs,
            **kwargs,
        )
        self.snowflake_conn_id = snowflake_conn_id
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.authenticator = authenticator
        self.session_parameters = session_parameters

    def execute_callable(self):
        hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )
        session = hook.get_snowpark_session()
        context = get_current_context()
        session.update_query_tag(
            {
                "dag_id": context["dag_run"].dag_id,
                "dag_run_id": context["dag_run"].run_id,
                "task_id": context["task_instance"].task_id,
                "operator": self.__class__.__name__,
            }
        )
        try:
            # inject session object if the function has "session" keyword as an argument
            self.op_kwargs = inject_session_into_op_kwargs(
                self.python_callable, dict(self.op_kwargs), session
            )
            return super().execute_callable()
        finally:
            session.close()
