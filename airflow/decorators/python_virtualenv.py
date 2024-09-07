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

from typing import TYPE_CHECKING, Callable

from airflow.decorators.base import task_decorator_factory
from airflow.decorators.python import _PythonDecoratedOperator
from airflow.providers.standard.core.operators.python import PythonVirtualenvOperator

if TYPE_CHECKING:
    from airflow.decorators.base import TaskDecorator


class _PythonVirtualenvDecoratedOperator(_PythonDecoratedOperator, PythonVirtualenvOperator):
    """Wraps a Python callable and captures args/kwargs when called for execution."""

    template_fields = PythonVirtualenvOperator.template_fields
    custom_operator_name: str = "@task.virtualenv"


def virtualenv_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Wrap a callable into an Airflow operator to run via a Python virtual environment.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    This function is only used only used during type checking or auto-completion.

    :meta private:

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys.
        Defaults to False.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PythonVirtualenvDecoratedOperator,
        **kwargs,
    )
