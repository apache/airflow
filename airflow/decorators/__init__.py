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

from typing import Callable, Optional

from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import _virtualenv_task
from airflow.decorators.task_group import task_group  # noqa
from airflow.exceptions import AirflowException
from airflow.models.dag import dag  # noqa
from airflow.providers_manager import ProvidersManager


class _TaskDecorator:
    def __init__(self):
        self.store = {"python": python_task, "virtualenv": _virtualenv_task}
        decorator = ProvidersManager().taskflow_decorators
        for decorator_name, decorator_class in decorator.items():
            self.store[decorator_name] = decorator_class

    def __call__(
        self, python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
    ):
        return self.store["python"](
            python_callable=python_callable, multiple_outputs=multiple_outputs, **kwargs
        )

    def __getattr__(self, name):
        if self.store.get(name, None):
            return self.store[name]
        raise AirflowException("Decorator %s not found", name)


task = _TaskDecorator()
