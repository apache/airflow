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
TaskFlow decorator for LLM SQL generation.

The user writes a function that **returns the prompt**. The decorator handles
the LLM call, schema introspection, and safety validation. The decorated task's
XCom output is the generated SQL string.
"""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
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


class _LLMSQLDecoratedOperator(DecoratedOperator, LLMSQLQueryOperator):
    """
    Wraps a callable that returns a prompt for LLM SQL generation.

    The user function is called at execution time to produce the prompt string.
    All other parameters (``llm_conn_id``, ``db_conn_id``, ``table_names``, etc.)
    are passed through to :class:`~airflow.providers.common.ai.operators.llm_sql.LLMSQLQueryOperator`.

    :param python_callable: A reference to a callable that returns the prompt string.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *LLMSQLQueryOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name: str = "@task.llm_sql"

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
            raise TypeError("The returned value from the @task.llm_sql callable must be a non-empty string.")

        self.render_template_fields(context)
        # Call LLMSQLQueryOperator.execute directly, not super().execute(),
        # because we need to skip DecoratedOperator.execute — the callable
        # invocation is already handled above.
        return LLMSQLQueryOperator.execute(self, context)


def llm_sql_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a function that returns a natural language prompt into an LLM SQL task.

    The function body constructs the prompt (can use Airflow context, XCom, etc.).
    The decorator handles: LLM connection, schema introspection, SQL generation,
    and safety validation.

    Usage::

        @task.llm_sql(
            llm_conn_id="openai_default",
            db_conn_id="postgres_default",
            table_names=["customers", "orders"],
        )
        def build_query(ds=None):
            return f"Find top 10 customers by revenue in {ds}"

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_LLMSQLDecoratedOperator,
        **kwargs,
    )
