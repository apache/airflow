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
"""TaskFlow decorator for LLM-backed file analysis."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
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


class _LLMFileAnalysisDecoratedOperator(DecoratedOperator, LLMFileAnalysisOperator):
    """Wrap a callable that returns the prompt string for file analysis."""

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMFileAnalysisOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_file_analysis"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
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
                "The returned value from the @task.llm_file_analysis callable must be a non-empty string."
            )

        self.render_template_fields(context)
        return LLMFileAnalysisOperator.execute(self, context)


def llm_file_analysis_task(
    python_callable: Callable | None = None,
    **kwargs: Any,
) -> TaskDecorator:
    """
    Wrap a callable that returns a prompt into an LLM-backed file-analysis task.

    Any file-analysis keyword arguments accepted by
    :class:`~airflow.providers.common.ai.operators.llm_file_analysis.LLMFileAnalysisOperator`,
    including ``sample_rows``, can be passed through this decorator.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMFileAnalysisDecoratedOperator,
        **kwargs,
    )
