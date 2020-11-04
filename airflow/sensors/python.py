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
from typing import Callable, Dict, List, Optional

from airflow.operators.python import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class PythonSensor(BaseSensorOperator):
    """
    Waits for a Python callable to return True.

    User could put input argument in templates_dict
    e.g ``templates_dict = {'start_ds': 1970}``
    and access the argument by calling ``kwargs['templates_dict']['start_ds']``
    in the callable

    :param python_callable: A reference to an object that is callable
    :type python_callable: python callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked
        in your function
    :type op_kwargs: dict
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable
    :type op_args: list
    :param templates_dict: a dictionary where the values are templates that
        will get templated by the Airflow engine sometime between
        ``__init__`` and ``execute`` takes place and are made available
        in your callable's context after the template has been applied.
    :type templates_dict: dict of str
    """

    template_fields = ('templates_dict', 'op_args', 'op_kwargs')

    @apply_defaults
    def __init__(
        self,
        *,
        python_callable: Callable,
        op_args: Optional[List] = None,
        op_kwargs: Optional[Dict] = None,
        templates_dict: Optional[Dict] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.templates_dict = templates_dict

    def poke(self, context: Dict):
        context.update(self.op_kwargs)
        context['templates_dict'] = self.templates_dict
        self.op_kwargs = PythonOperator.determine_op_kwargs(self.python_callable, context, len(self.op_args))

        self.log.info("Poking callable: %s", str(self.python_callable))
        return_value = self.python_callable(*self.op_args, **self.op_kwargs)
        return bool(return_value)
