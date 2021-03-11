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

import functools
from typing import Callable, Dict, Optional, TypeVar

from airflow.decorators.base import _BaseDecoratedOperator, base_task
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults


class _PythonDecoratedOperator(_BaseDecoratedOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function (templated)
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :type op_args: list
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    template_fields = ('op_args', 'op_kwargs')
    template_fields_renderers = {"op_args": "py", "op_kwargs": "py"}

    ui_color = PythonOperator.ui_color

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs = ('python_callable',)

    @apply_defaults
    def __init__(
        self,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

    def execute(self, context: Dict):
        return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        self.log.debug("Done. Returned value was: %s", return_value)
        if not self.multiple_outputs:
            return return_value
        if isinstance(return_value, dict):
            for key in return_value.keys():
                if not isinstance(key, str):
                    raise AirflowException(
                        'Returned dictionary keys must be strings when using '
                        f'multiple_outputs, found {key} ({type(key)}) instead'
                    )
            for key, value in return_value.items():
                self.xcom_push(context, key, value)
        else:
            raise AirflowException(
                f'Returned output was type {type(return_value)} expected dictionary ' 'for multiple_outputs'
            )
        return return_value


T = TypeVar("T", bound=Callable)  # pylint: disable=invalid-name


def task(python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.
    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    @functools.wraps(python_callable)
    def wrapper(f):
        return base_task(
            python_callable=f,
            multiple_outputs=multiple_outputs,
            impl_class=_PythonDecoratedOperator,
            **kwargs,
        )

    if callable(python_callable):
        return wrapper(python_callable)
    elif python_callable is not None:
        raise AirflowException('No args allowed while using @task, use kwargs instead')
    return wrapper
