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
TaskFlow decorator for LLM-driven data-quality checks.

The user writes a function that **returns the prompts dict** — a mapping of
``{check_name: natural_language_description}``.  The decorator handles LLM
plan generation, plan caching, SQL execution against the target database, and
metric validation.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_data_quality import LLMDataQualityOperator
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


class _LLMDQDecoratedOperator(DecoratedOperator, LLMDataQualityOperator):
    """
    Wraps a callable that returns a prompts dict for LLM data-quality checks.

    The user function is called at execution time to produce the prompts dict.
    All other parameters (``llm_conn_id``, ``db_conn_id``, ``table_names``,
    ``validators``, etc.) are passed through to
    :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator`.

    :param python_callable: A callable that returns a ``dict[str, str]`` mapping
        check names to natural-language descriptions.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMDataQualityOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_dq"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        # prompts will be populated by the callable at execution time.
        # _validate_validator_keys is skipped at init when prompts is not a dict.
        super().__init__(
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
            prompts=SET_DURING_EXECUTION,
            **kwargs,
        )

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.prompts = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.prompts, dict) or not self.prompts:
            raise TypeError(
                "The returned value from the @task.llm_dq callable must be a non-empty dict[str, str]."
            )

        # Now that prompts is populated, validate that all validator keys are present.
        self._validate_validator_keys()

        self.render_template_fields(context)
        return LLMDataQualityOperator.execute(self, context)


def llm_dq_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a prompts dict into an LLM data-quality task.

    The function body builds the ``{check_name: description}`` mapping (can use
    Airflow context, XCom, etc.). The decorator handles: LLM plan generation,
    plan caching, SQL execution against the target database, and metric
    validation.

    Usage::

        from airflow.providers.common.ai.utils.dq_validation import (
            null_pct_check,
            row_count_check,
        )


        @task.llm_dq(
            llm_conn_id="openai_default",
            db_conn_id="postgres_default",
            table_names=["orders", "customers"],
            validators={
                "row_count": row_count_check(min_count=1000),
                "email_nulls": null_pct_check(max_pct=0.05),
            },
        )
        def dq_checks(ds=None):
            return {
                "row_count": f"The orders table must have at least 1000 rows as of {ds}.",
                "email_nulls": "No more than 5% of customer emails should be null.",
            }

    With ``dry_run=True`` to preview the generated plan before execution::

        @task.llm_dq(
            llm_conn_id="openai_default",
            db_conn_id="postgres_default",
            table_names=["orders"],
            dry_run=True,
        )
        def preview_plan():
            return {"row_count": "The orders table must have at least 1000 rows."}

    :param python_callable: Function to decorate.  Must return a non-empty
        ``dict[str, str]`` mapping check names to natural-language descriptions.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMDQDecoratedOperator,
        **kwargs,
    )
