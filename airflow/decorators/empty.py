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

from airflow.operators.empty import EmptyOperator
from airflow.decorators.base import task_decorator_factory
from typing import Callable, Any


class _EmptyDecoratedOperator(EmptyOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable. If provided, it will be executed
        when the task runs.
    :param task_id: Task ID.
    :param op_args: A list of positional arguments that will be passed to the callable (templated).
    :param op_kwargs: A dictionary of keyword arguments that will be passed to the callable (templated).
    :param kwargs: Additional keyword arguments that will be passed to the base EmptyOperator.

    Example Usage:
        @task.empty
        def start_task():
            print("Starting the workflow.")

        @task.empty
        def end_task():
            print("Workflow complete.")
    """
    custom_operator_name: str = "@task.empty"

    def __init__(
        self,
        *,
        python_callable: Callable | None = None,
        op_args: list[Any] | None = None,
        op_kwargs: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        # Remove arguments that are not valid for EmptyOperator
        kwargs.pop("op_args", None)
        kwargs.pop("op_kwargs", None)

        super().__init__(**kwargs)
        self.python_callable = python_callable

    def execute(self, context: Any) -> None:
        """Executes the Python callable if provided, otherwise does nothing."""
        if self.python_callable:
            self.python_callable()
        self.log.info("Executing a custom empty task.")

def empty_task(
    python_callable: Callable | None = None,
    **kwargs,
) -> Callable:
    """
    Wrap a function into a EmptyOperator.

    :param python_callable: Function to decorate.
    """
    return task_decorator_factory(
        python_callable=python_callable,
        decorated_operator_class=_EmptyDecoratedOperator,
        **kwargs,
    )
