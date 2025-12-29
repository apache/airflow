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

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    task_decorator_factory,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.operator_helpers import determine_kwargs

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class _SQLDecoratedOperator(DecoratedOperator, SQLExecuteQueryOperator):
    """
    Wraps a Python callable and uses the callable return value as the SQL command to be executed.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked (templated).
    :param op_args: A list of positional arguments that will get unpacked (templated).
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *SQLExecuteQueryOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **SQLExecuteQueryOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.sql"

    def __init__(
        self,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            # TODO: Comeback and add more, such as sql_conn, etc.
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        # Set the sql
        self.sql = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.sql, str) or self.sql.strip() == "":
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")

        context["ti"].render_templates()  # type: ignore[attr-defined]

        return super().execute(context)


def sql_task(python_callable=None, **kwargs) -> TaskDecorator:
    """
    Wrap a Python function into a SQLExecuteQueryOperator.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SQLDecoratedOperator,
        **kwargs,
    )
