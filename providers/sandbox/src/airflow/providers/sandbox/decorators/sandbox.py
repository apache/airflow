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
"""``@task.sandbox`` — TaskFlow decorator mirroring ``@task.bash``.

The decorated callable returns a shell command (a string); that command is then
executed inside an ephemeral cloud sandbox by :class:`SandboxOperator`. This is
the idiomatic way to run a step in a sandbox — e.g. an LLM/agent script with
credentials injected via ``env`` — without switching the whole executor.
"""

from __future__ import annotations

import warnings
from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    determine_kwargs,
    task_decorator_factory,
)
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION

from airflow.providers.sandbox.operators.sandbox import SandboxOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class _SandboxDecoratedOperator(DecoratedOperator, SandboxOperator):
    """Use the callable's return value as the command to run in a sandbox."""

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *SandboxOperator.template_fields,
    )
    custom_operator_name: str = "@task.sandbox"
    overwrite_rtif_after_execution: bool = True

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if kwargs.pop("multiple_outputs", None):
            warnings.warn(
                f"`multiple_outputs=True` is not supported in {self.custom_operator_name} "
                "tasks. Ignoring.",
                UserWarning,
                stacklevel=3,
            )
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            command=SET_DURING_EXECUTION,
            multiple_outputs=False,
            **kwargs,
        )

    def execute(self, context: "Context") -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        self.command = self.python_callable(*self.op_args, **kwargs)
        if not isinstance(self.command, str) or self.command.strip() == "":
            raise TypeError("The returned value from a @task.sandbox callable must be a non-empty string.")
        self.render_template_fields(context)
        return super().execute(context)


def sandbox_task(python_callable: Callable | None = None, **kwargs: Any) -> TaskDecorator:
    """Wrap a function whose return value is run as a command in a sandbox.

    :meta private:
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_SandboxDecoratedOperator,
        **kwargs,
    )
