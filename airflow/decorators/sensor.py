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

from inspect import signature
from typing import Any, Callable, Collection, Dict, Iterable, Mapping, Optional, Tuple

from airflow.decorators.base import get_unique_task_id, task_decorator_factory
from airflow.models.taskinstance import Context
from airflow.sensors.base import BaseSensorOperator


class DecoratedSensorOperator(BaseSensorOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param task_id: task Id
    :type task_id: str
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param kwargs_to_upstream: For certain operators, we might need to upstream certain arguments
        that would otherwise be absorbed by the DecoratedOperator (for example python_callable for the
        PythonOperator). This gives a user the option to upstream kwargs as needed.
    :type kwargs_to_upstream: dict
    """

    template_fields: Iterable[str] = ('op_args', 'op_kwargs')
    template_fields_renderers: Dict[str, str] = {"op_args": "py", "op_kwargs": "py"}

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Tuple[str, ...] = ('python_callable',)

    def __init__(
        self,
        *,
        python_callable: Callable,
        task_id: str,
        op_args: Collection[Any],
        op_kwargs: Mapping[str, Any],
        **kwargs,
    ) -> None:
        kwargs.pop('multiple_outputs')
        kwargs['task_id'] = get_unique_task_id(task_id, kwargs.get('dag'), kwargs.get('task_group'))
        self.python_callable = python_callable
        # Check that arguments can be binded
        signature(python_callable).bind(*op_args, **op_kwargs)
        self.op_args = op_args
        self.op_kwargs = op_kwargs
        super().__init__(**kwargs)

    def poke(self, context: Context) -> bool:
        return self.python_callable(*self.op_args, **self.op_kwargs)


def sensor(python_callable: Optional[Callable] = None, **kwargs):
    """
    Wraps a function into an Airflow operator.

    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=DecoratedSensorOperator,
        **kwargs,
    )
