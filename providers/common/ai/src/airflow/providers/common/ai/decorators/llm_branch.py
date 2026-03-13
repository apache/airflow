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
"""
TaskFlow decorator for LLM-driven branching.

The user writes a function that **returns the prompt string**. The decorator
discovers downstream tasks from the DAG topology and asks the LLM to choose
which branch(es) to execute using pydantic-ai structured output.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    task_decorator_factory,
)
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION
from airflow.utils.operator_helpers import determine_kwargs

if TYPE_CHECKING:
    from airflow.sdk import Context


class _LLMBranchDecoratedOperator(DecoratedOperator, LLMBranchOperator):
    """
    Wraps a callable that returns a prompt for LLM-driven branching.

    The user function is called at execution time to produce the prompt string.
    All other parameters (``llm_conn_id``, ``system_prompt``, ``allow_multiple_branches``,
    etc.) are passed through to
    :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`.

    :param python_callable: A reference to a callable that returns the prompt string.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMBranchOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_branch"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            prompt=SET_DURING_EXECUTION,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.prompt = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.prompt, str) or not self.prompt.strip():
            raise TypeError(
                "The returned value from the @task.llm_branch callable must be a non-empty string."
            )

        self.render_template_fields(context)
        return LLMBranchOperator.execute(self, context)


def llm_branch_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a prompt into an LLM-driven branching task.

    The function body constructs the prompt. The decorator discovers downstream
    tasks from the DAG topology and asks the LLM to choose which branch(es)
    to execute.

    Usage::

        @task.llm_branch(
            llm_conn_id="openai_default",
            system_prompt="Route support tickets to the right team.",
        )
        def route_ticket(message: str):
            return f"Route this ticket: {message}"

    With multiple branches::

        @task.llm_branch(
            llm_conn_id="openai_default",
            system_prompt="Select all applicable categories.",
            allow_multiple_branches=True,
        )
        def classify(text: str):
            return f"Classify this text: {text}"

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMBranchDecoratedOperator,
        **kwargs,
    )
