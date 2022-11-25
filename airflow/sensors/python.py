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

from typing import Any, Callable, Mapping, Sequence

from airflow.sensors.base import BaseSensorOperator, PokeReturnValue
from airflow.utils.context import Context, context_merge
from airflow.utils.operator_helpers import determine_kwargs


class PythonSensor(BaseSensorOperator):
    """
    Waits for a Python callable to return True.

    User could put input argument in templates_dict
    e.g ``templates_dict = {'start_ds': 1970}``
    and access the argument by calling ``kwargs['templates_dict']['start_ds']``
    in the callable

    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/operator:PythonSensor`
    """

    template_fields: Sequence[str] = ("templates_dict", "op_args", "op_kwargs")

    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: list | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        templates_dict: dict | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict

    def poke(self, context: Context) -> PokeReturnValue:
        context_merge(context, self.op_kwargs, templates_dict=self.templates_dict)
        self.op_kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.log.info("Poking callable: %s", str(self.python_callable))
        return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        return PokeReturnValue(bool(return_value))
