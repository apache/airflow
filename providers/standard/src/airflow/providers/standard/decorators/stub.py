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

import ast
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class _StubOperator(DecoratedOperator):
    custom_operator_name: str = "@task.stub"

    def __init__(
        self,
        *,
        python_callable: Callable,
        task_id: str,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            task_id=task_id,
            **kwargs,
        )
        # Validate python callable
        module = ast.parse(self.get_python_source())

        if len(module.body) != 1:
            raise RuntimeError("Expected a single statement")
        fn = module.body[0]
        if not isinstance(fn, ast.FunctionDef):
            raise RuntimeError("Expected a single sync function")
        for stmt in fn.body:
            if isinstance(stmt, ast.Pass):
                continue
            if isinstance(stmt, ast.Expr):
                if isinstance(stmt.value, ast.Constant) and isinstance(stmt.value.value, (str, type(...))):
                    continue

            raise ValueError(
                f"Functions passed to @task.stub must be an empty function (`pass`, or `...` only) (got {stmt})"
            )

        ...

    def execute(self, context: Context) -> Any:
        raise RuntimeError(
            "@task.stub should not be executed directly -- we expected this to go to a remote worker. "
            "Check your pool and worker configs"
        )


def stub(
    python_callable: Callable | None = None,
    queue: str | None = None,
    executor: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """
    Define a stub task in the DAG.

    Stub tasks exist in the Dag graph only, but the execution must happen in an external
    environment via the Task Execution Interface.

    """
    return task_decorator_factory(
        decorated_operator_class=_StubOperator,
        python_callable=python_callable,
        queue=queue,
        executor=executor,
        **kwargs,
    )
