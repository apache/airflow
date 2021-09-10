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

from airflow.decorators.python import PythonDecoratorMixin, python_task  # noqa
from airflow.decorators.python_virtualenv import PythonVirtualenvDecoratorMixin
from airflow.decorators.task_group import task_group  # noqa
from airflow.exceptions import AirflowException
from airflow.models.dag import dag  # noqa
from airflow.providers_manager import ProvidersManager

# [START import_docker]
try:
    from airflow.providers.docker.decorators.docker import DockerDecoratorMixin
except ImportError:
    DockerDecoratorMixin = None
# [END import_docker]


class _TaskDecorator:
    def __init__(self):
        self.store = {}
        decorator = ProvidersManager().taskflow_decorators
        for decorator_name, decorator_class in decorator.items():
            self.store[decorator_name] = decorator_class

    def __getattr__(self, name):
        if self.store.get(name, None):
            return self.store[name]
        raise AirflowException("Decorator %s not found", name)


_target = _TaskDecorator

classes_to_mixin = [PythonDecoratorMixin, DockerDecoratorMixin]


class _PythonTask(_target, PythonDecoratorMixin):
    pass


_target = _PythonTask


class _VirtualenvTask(_target, PythonVirtualenvDecoratorMixin):
    pass


_target = _VirtualenvTask

# [START extend_docker]
if DockerDecoratorMixin:

    class _DockerTask(_target, DockerDecoratorMixin):
        pass

    _target = _DockerTask
task = _target()
# [END extend_docker]
