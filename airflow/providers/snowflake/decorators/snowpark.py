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

from typing import TYPE_CHECKING, Callable, Sequence

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator

try:
    import snowflake.snowpark  # noqa
except ImportError:
    raise AirflowException(
        "The snowflake-snowpark-python package is not installed. Make sure you are using Python 3.8."
    )


class _SnowparkDecoratedOperator(DecoratedOperator, PythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    """

    custom_operator_name = "@task.snowpark"

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        parameters: dict | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        python_callable,
        op_args,
        op_kwargs: dict,
        **kwargs,
    ) -> None:
        self.snowflake_conn_id = snowflake_conn_id
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters

        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            # airflow.decorators.base.DecoratedOperator checks if the functions are bindable, so we have to
            # add an artificial value to pass the validations. The real value is determined at runtime.
            op_kwargs={**op_kwargs, "snowpark_session": None},
            **kwargs,
        )

    def execute_callable(self):
        hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            parameters=self.parameters,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
        )
        snowpark_session = hook.get_snowpark_session()
        try:
            return self.python_callable(
                *self.op_args, **{**self.op_kwargs, "snowpark_session": snowpark_session}
            )
        finally:
            snowpark_session.close()


def snowpark_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wraps a function into an Airflow operator.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_SnowparkDecoratedOperator,
        **kwargs,
    )
