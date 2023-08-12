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

from typing import Callable, Sequence

from airflow.decorators.base import TaskDecorator, get_unique_task_id, task_decorator_factory
from airflow.sensors.python import PythonSensor


class DecoratedSensorOperator(PythonSensor):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :param task_id: task Id
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :param kwargs_to_upstream: For certain operators, we might need to upstream certain arguments
        that would otherwise be absorbed by the DecoratedOperator (for example python_callable for the
        PythonOperator). This gives a user the option to upstream kwargs as needed.
    """

    template_fields: Sequence[str] = ("op_args", "op_kwargs")
    template_fields_renderers: dict[str, str] = {"op_args": "py", "op_kwargs": "py"}

    custom_operator_name = "@task.sensor"

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

    def __init__(
        self,
        *,
        task_id: str,
        **kwargs,
    ) -> None:
        kwargs.pop("multiple_outputs")
        kwargs["task_id"] = get_unique_task_id(task_id, kwargs.get("dag"), kwargs.get("task_group"))
        super().__init__(**kwargs)


def sensor_task(python_callable: Callable | None = None, **kwargs) -> TaskDecorator:
    """
    Wrap a function into an Airflow operator.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.
    :param python_callable: Function to decorate
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=DecoratedSensorOperator,
        **kwargs,
    )
