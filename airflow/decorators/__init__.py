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

from typing import TYPE_CHECKING

from airflow.decorators.python import PythonDecoratorMixin, python_task  # noqa
from airflow.decorators.python_virtualenv import PythonVirtualenvDecoratorMixin
from airflow.decorators.task_group import task_group  # noqa
from airflow.models.dag import dag  # noqa
from airflow.providers_manager import ProvidersManager


class _TaskDecorator(PythonDecoratorMixin, PythonVirtualenvDecoratorMixin):
    def __getattr__(self, name):
        if name.startswith("__"):
            raise AttributeError(f'{type(self).__name__} has no attribute {name!r}')
        decorators = ProvidersManager().taskflow_decorators
        if name not in decorators:
            raise AttributeError(f"task decorator {name!r} not found")
        return decorators[name]


# [START mixin_for_autocomplete]
if TYPE_CHECKING:
    try:
        from airflow.providers.docker.decorators.docker import DockerDecoratorMixin

        class _DockerTask(_TaskDecorator, DockerDecoratorMixin):
            pass

        _TaskDecorator = _DockerTask
    except ImportError:
        pass
# [END mixin_for_autocomplete]

task = _TaskDecorator()
