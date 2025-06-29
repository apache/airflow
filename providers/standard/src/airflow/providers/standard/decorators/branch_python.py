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

from collections.abc import Callable
from typing import TYPE_CHECKING

from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk.bases.decorator import task_decorator_factory
else:
    from airflow.decorators.base import task_decorator_factory  # type: ignore[no-redef]
from airflow.providers.standard.decorators.python import _PythonDecoratedOperator
from airflow.providers.standard.operators.python import BranchPythonOperator

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import TaskDecorator


class _BranchPythonDecoratedOperator(_PythonDecoratedOperator, BranchPythonOperator):
    """Wraps a Python callable and captures args/kwargs when called for execution."""

    template_fields = BranchPythonOperator.template_fields
    custom_operator_name: str = "@task.branch"


def branch_task(
    python_callable: Callable | None = None, multiple_outputs: bool | None = None, **kwargs
) -> TaskDecorator:
    """
    Wrap a python function into a BranchPythonOperator.

    For more information on how to use this operator, take a look at the guide:
    :ref:`concepts:branching`

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_BranchPythonDecoratedOperator,
        **kwargs,
    )
