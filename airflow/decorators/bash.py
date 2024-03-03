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
from typing import Any, Callable, Collection, Mapping, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.operators.bash import BashOperator
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.types import NOTSET


class _BashDecoratedOperator(DecoratedOperator, BashOperator):
    """Wraps a Python callable and uses the callable return value as the Bash command to be executed.

    :param python_callable: A reference to an object that is callable.
    :param op_kwargs: A dictionary of keyword arguments that will get unpacked
        in your function (templated).
    :param op_args: A list of positional arguments that will get unpacked when
        calling your callable (templated).
    """

    template_fields: Sequence[str] = (*DecoratedOperator.template_fields, *BashOperator.template_fields)
    template_fields_renderers: dict[str, str] = {
        **DecoratedOperator.template_fields_renderers,
        **BashOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.bash"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        if kwargs.get("multiple_outputs") is not None:
            warnings.warn(
                f"`multiple_outputs` is not supported in {self.custom_operator_name} tasks. Ignoring.",
                stacklevel=1,
            )
        kwargs.pop("multiple_outputs")

        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            bash_command=NOTSET,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.bash_command = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.bash_command, str) or self.bash_command.strip() == "":
            raise TypeError("The returned value from the TaskFlow callable must be a non-empty string.")

        return super().execute(context)


def bash_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """Wrap a function into a BashOperator.

    Accepts kwargs for operator kwargs. Can be reused in a single DAG. This function is only used only used
    during type checking or auto-completion.

    :param python_callable: Function to decorate.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_BashDecoratedOperator,
        **kwargs,
    )
