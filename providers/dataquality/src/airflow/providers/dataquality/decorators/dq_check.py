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
"""TaskFlow decorator for :class:`~airflow.providers.dataquality.operators.dq_check.DQCheckOperator`."""

from __future__ import annotations

from collections.abc import Callable, Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    context_merge,
    determine_kwargs,
    task_decorator_factory,
)
from airflow.providers.dataquality.operators.dq_check import DQCheckOperator
from airflow.providers.dataquality.rules import RuleSet
from airflow.sdk.definitions._internal.types import SET_DURING_EXECUTION

if TYPE_CHECKING:
    from airflow.sdk import Context
    from airflow.sdk.bases.decorator import TaskDecorator


class _DQCheckDecoratedOperator(DecoratedOperator, DQCheckOperator):
    """
    Wraps a callable that optionally returns a runtime ruleset for a data quality check.

    :param python_callable: A reference to a callable returning ``None`` or a ruleset for this run.
    :param op_args: Positional arguments for the callable.
    :param op_kwargs: Keyword arguments for the callable.
    """

    template_fields: Sequence[str] = (
        *DecoratedOperator.template_fields,
        *DQCheckOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict[str, str]] = {
        **DecoratedOperator.template_fields_renderers,
    }

    custom_operator_name = "@task.dq_check"

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        kwargs.setdefault("ruleset", SET_DURING_EXECUTION)
        super().__init__(python_callable=python_callable, op_args=op_args, op_kwargs=op_kwargs, **kwargs)

    def execute(self, context: Context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)
        ruleset = self.python_callable(*self.op_args, **kwargs)
        if ruleset is not None:
            if not isinstance(ruleset, (RuleSet, dict, str)):
                raise TypeError(
                    f"{self.custom_operator_name} function must return a RuleSet, dict, path, or None, "
                    f"got {type(ruleset).__name__}"
                )
            self.ruleset = ruleset
        return DQCheckOperator.execute(self, context)


def dq_check_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Turn a function into a data quality check task.

    ``ruleset`` may be passed as a decorator argument when known at Dag-parse time, or returned
    by the decorated function at runtime. ``table`` or ``asset`` are passed through ``kwargs``
    exactly as on :class:`~airflow.providers.dataquality.operators.dq_check.DQCheckOperator`. The
    function itself is optional plumbing: return ``None`` to run the check as declared, or a
    ruleset to use for this run.

    Usage::

        @task.dq_check(conn_id="warehouse", ruleset=rules)
        def orders_quality(ds=None):
            return None

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_DQCheckDecoratedOperator,
        **kwargs,
    )
