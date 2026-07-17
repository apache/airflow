#
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

import inspect
from collections.abc import Callable, Sequence

from airflow.providers.apache.spark.operators.spark_pyspark import SPARK_CONTEXT_KEYS, PySparkOperator
from airflow.providers.common.compat.sdk import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)


class _PySparkDecoratedOperator(DecoratedOperator, PySparkOperator):
    custom_operator_name = "@task.pyspark"

    def __init__(
        self,
        *,
        python_callable: Callable,
        conn_id: str | None = None,
        config_kwargs: dict | None = None,
        op_args: Sequence | None = None,
        op_kwargs: dict | None = None,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }

        signature = inspect.signature(python_callable)
        parameters = [
            param.replace(default=None) if param.name in SPARK_CONTEXT_KEYS else param
            for param in signature.parameters.values()
        ]
        # mypy does not understand __signature__ attribute
        # see https://github.com/python/mypy/issues/12472
        python_callable.__signature__ = signature.replace(parameters=parameters)  # type: ignore[attr-defined]

        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            config_kwargs=config_kwargs,
            conn_id=conn_id,
            op_args=op_args,
            op_kwargs=op_kwargs,
            **kwargs,
        )


def pyspark_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_PySparkDecoratedOperator,
        **kwargs,
    )
