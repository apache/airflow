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

from collections.abc import Callable, Sequence

from airflow.providers.common.compat.sdk import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.providers.snowflake.operators.snowpark import SnowparkOperator
from airflow.providers.snowflake.utils.snowpark import inject_session_into_op_kwargs


class _SnowparkDecoratedOperator(DecoratedOperator, SnowparkOperator):
    """
    Wraps a Python callable that contains Snowpark code and captures args/kwargs when called for execution.

    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param python_callable: A reference to an object that is callable
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
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
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """

    custom_operator_name = "@task.snowpark"

    def __init__(
        self,
        *,
        snowflake_conn_id: str = "snowflake_default",
        python_callable: Callable,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            snowflake_conn_id=snowflake_conn_id,
            python_callable=python_callable,
            op_args=op_args,
            # airflow.decorators.base.DecoratedOperator checks if the functions are bindable, so we have to
            # add an artificial value to pass the validation if there is a keyword argument named `session`
            # in the signature of the python callable. The real value is determined at runtime.
            op_kwargs=inject_session_into_op_kwargs(python_callable, op_kwargs, None)
            if op_kwargs is not None
            else op_kwargs,
            warehouse=warehouse,
            database=database,
            role=role,
            schema=schema,
            authenticator=authenticator,
            session_parameters=session_parameters,
            **kwargs,
        )


def snowpark_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that contains Snowpark code into an Airflow operator.

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
