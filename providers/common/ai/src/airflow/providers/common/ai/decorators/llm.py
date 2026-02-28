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
TaskFlow decorator for general-purpose LLM calls.

The user writes a function that **returns the prompt string**. The decorator
handles hook creation, agent configuration, LLM call, and output serialization.
When ``output_type`` is a Pydantic ``BaseModel``, the result is serialized via
``model_dump()`` for XCom.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm import LLMOperator
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


class _LLMDecoratedOperator(DecoratedOperator, LLMOperator):
    """
    Wraps a callable that returns a prompt for a general-purpose LLM call.

    The user function is called at execution time to produce the prompt string.
    All other parameters (``llm_conn_id``, ``model_id``, ``system_prompt``, etc.)
    are passed through to :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`.

    :param python_callable: A reference to a callable that returns the prompt string.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm"

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
            raise TypeError("The returned value from the @task.llm callable must be a non-empty string.")

        self.render_template_fields(context)
        return LLMOperator.execute(self, context)


def llm_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a prompt into a general-purpose LLM task.

    The function body constructs the prompt (can use Airflow context, XCom, etc.).
    The decorator handles hook creation, agent configuration, LLM call, and output
    serialization.

    Usage::

        @task.llm(
            llm_conn_id="openai_default",
            system_prompt="Summarize concisely.",
        )
        def summarize(text: str):
            return f"Summarize this article: {text}"

    With structured output::

        @task.llm(
            llm_conn_id="openai_default",
            system_prompt="Extract named entities.",
            output_type=Entities,
        )
        def extract(text: str):
            return f"Extract entities from: {text}"

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMDecoratedOperator,
        **kwargs,
    )
