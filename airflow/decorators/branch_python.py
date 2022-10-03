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
from textwrap import dedent
from typing import Callable, Sequence

from airflow.decorators.base import DecoratedOperator, TaskDecorator, task_decorator_factory
from airflow.operators.python import BranchPythonOperator
from airflow.utils.decorators import remove_task_decorator


class _BranchPythonDecoratedOperator(DecoratedOperator, BranchPythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    """

    template_fields: Sequence[str] = ('op_args', 'op_kwargs')
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ('python_callable',)

    custom_operator_name: str = "@task.branch"

    def __init__(
        self,
        **kwargs,
    ) -> None:
        kwargs_to_upstream = {
            "python_callable": kwargs["python_callable"],
            "op_args": kwargs["op_args"],
            "op_kwargs": kwargs["op_kwargs"],
        }
        super().__init__(kwargs_to_upstream=kwargs_to_upstream, **kwargs)

    def get_python_source(self):
        raw_source = inspect.getsource(self.python_callable)
        res = dedent(raw_source)
        res = remove_task_decorator(res, "@task.branch")
        return res


def branch_task(
    python_callable: Callable | None = None, multiple_outputs: bool | None = None, **kwargs
) -> TaskDecorator:
    """
    Wraps a python function into a BranchPythonOperator
    For more information on how to use this operator, take a look at the guide:
    :ref:`howto/operator:BranchPythonOperator`

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
