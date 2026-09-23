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
TaskFlow decorator for ``@task.llm_batch``.

The user writes a function that **returns the batch's inputs** (a
``list[str]`` or ``list[BatchRequest]``), not a single prompt: a batch submits
many requests at once.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_batch import LLMBatchOperator
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    context_merge,
    determine_kwargs,
    task_decorator_factory,
)
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION

if TYPE_CHECKING:
    from airflow.sdk import Context


class _LLMBatchDecoratedOperator(DecoratedOperator, LLMBatchOperator):
    """
    Wraps a callable that returns the batch's inputs.

    The user function is called at execution time to produce
    ``list[str] | list[BatchRequest]``. All other parameters (``result_path``,
    ``llm_conn_id``, ``model_id``, ``output_type``, ...) are passed through to
    :class:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator`.

    ``python_callable`` must be deterministic across attempts: its return
    value feeds the input fingerprint that decides whether a retry re-attaches
    to the batch already submitted or treats the input as changed and pays
    for a new one. A callable that embeds ``datetime.now()`` looks like new
    input on every retry.

    Unlike ``@task.llm``, the returned prompts are **not** rendered as Jinja
    templates. Batch inputs are typically bulk text the Dag author did not
    write, where a stray ``{{`` or ``{%`` would either fail the whole batch or
    resolve ``var``/``conn`` accessors against Airflow secrets. Anything
    dynamic belongs in the callable, which receives the task context. The
    operator's other template fields (``result_path``, ``system_prompt``,
    ``model_id``, ``llm_conn_id``) are rendered as usual.

    :param python_callable: A reference to a callable that returns the batch's inputs.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = tuple(
        field
        for field in (*DecoratedOperator.template_fields, *LLMBatchOperator.template_fields)
        if field != "requests"
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_batch"

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
            requests=SET_DURING_EXECUTION,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        self.requests = self.python_callable(*self.op_args, **kwargs)
        return LLMBatchOperator.execute(self, context)


def llm_batch_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a batch's inputs into an ``@task.llm_batch`` task.

    The function body constructs the list of inputs (it can use Airflow
    context, XCom, etc.). Results are written to ``result_path`` as JSONL;
    the XCom value is a manifest describing where to find them (see
    :class:`~airflow.providers.common.ai.operators.llm_batch.LLMBatchOperator`).

    Usage::

        @task.llm_batch(
            llm_conn_id="pydanticai_default",
            model_id="openai:gpt-5",
            result_path="s3://bucket/prefix/{{ run_id }}",
        )
        def build_prompts(rows: list[str]) -> list[str]:
            return [f"Summarize: {row}" for row in rows]

    With structured output::

        @task.llm_batch(
            llm_conn_id="pydanticai_default",
            model_id="openai:gpt-5",
            result_path="s3://bucket/prefix/{{ run_id }}",
            output_type=Diagnosis,
        )
        def build_prompts(rows: list[str]) -> list[str]:
            return [f"Diagnose: {row}" for row in rows]

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMBatchDecoratedOperator,
        **kwargs,
    )
