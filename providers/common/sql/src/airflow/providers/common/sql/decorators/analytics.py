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
    AIRFLOW_V_3_0_PLUS,
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    task_decorator_factory,
)
from airflow.providers.common.sql.operators.analytics import AnalyticsOperator
from airflow.utils.operator_helpers import determine_kwargs

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
else:
    from airflow.utils.types import NOTSET as SET_DURING_EXECUTION  # type: ignore[attr-defined,no-redef]


if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class _AnalyticsDecoratedOperator(DecoratedOperator, AnalyticsOperator):
    """
    Wraps a Python callable and uses the callable return value as the SQL commands to be executed.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked (templated).
    :param op_args: A list of positional arguments that will get unpacked (templated).
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *AnalyticsOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
        **AnalyticsOperator.template_fields_renderers,
    }

    overwrite_rtif_after_execution: bool = True

    custom_operator_name: str = "@task.analytics"

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
            queries=SET_DURING_EXECUTION,
            **kwargs,
        )

    @property
    def xcom_push(self) -> bool:
        """Compatibility property for BaseDecorator that expects xcom_push attribute."""
        return self.do_xcom_push

    @xcom_push.setter
    def xcom_push(self, value: bool) -> None:
        """Compatibility setter for BaseDecorator that expects xcom_push attribute."""
        self.do_xcom_push = value

    def execute(self, context: Context) -> Any:
        """
        Build the SQL and execute the generated query (or queries).

        :param context: Airflow context.
        :return: Any
        """
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        # Set the queries using the Python callable
        result = self.python_callable(*self.op_args, **kwargs)

        # Only non-empty strings and non-empty lists of non-empty strings are acceptable return types
        if (
            not isinstance(result, (str, list))
            or (isinstance(result, str) and not result.strip())
            or (
                isinstance(result, list)
                and (not result or not all(isinstance(s, str) and s.strip() for s in result))
            )
        ):
            raise TypeError(
                "The returned value from the @task.analytics callable must be a non-empty string "
                "or a non-empty list of non-empty strings."
            )

        # AnalyticsOperator expects queries as a list of strings
        self.queries = [result] if isinstance(result, str) else result

        self.render_template_fields(context)

        return AnalyticsOperator.execute(self, context)


def analytics_task(python_callable=None, **kwargs) -> TaskDecorator:
    """
    Wrap a Python function into a AnalyticsOperator.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_AnalyticsDecoratedOperator,
        **kwargs,
    )
