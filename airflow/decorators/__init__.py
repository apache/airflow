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

from typing import TYPE_CHECKING, Callable, Optional, TypeVar, overload

from airflow.decorators.base import TaskDecorator
from airflow.decorators.python import python_task
from airflow.decorators.python_virtualenv import virtualenv_task
from airflow.decorators.task_group import task_group
from airflow.models.dag import dag
from airflow.providers_manager import ProvidersManager

__all__ = ["dag", "task", "task_group", "python_task", "virtualenv_task"]

T = TypeVar("T", bound=Callable)


class _TaskDecoratorMeta(type):
    def __getattr__(self, name: str) -> "TaskDecorator":
        """Dynamically get provider-registered task decorators, e.g. ``@task.docker``."""
        if name.startswith("__"):
            raise AttributeError(f'{self.__name__} has no attribute {name!r}')
        decorators = ProvidersManager().taskflow_decorators
        if name not in decorators:
            raise AttributeError(f"task decorator {name!r} not found")
        return decorators[name]


class _TaskDecoratorAutocompletion(metaclass=_TaskDecoratorMeta):
    # [START mixin_for_autocomplete]
    if TYPE_CHECKING:
        from airflow.providers.docker.decorators.docker import docker_task_autocomplete as docker
    # [END mixin_for_autocomplete]


class task(_TaskDecoratorAutocompletion):
    """Implementation to provide the ``@task`` syntax."""

    python = python_task
    virtualenv = virtualenv_task

    @overload
    def __new__(  # type: ignore[misc]
        cls,
        multiple_outputs: Optional[bool] = None,
        **kwargs,
    ) -> TaskDecorator:
        """For the decorator factory ``@task()`` case."""

    @overload
    def __new__(cls, python_callable: T) -> T:  # type: ignore[misc]
        """For the "bare decorator" ``@task`` case."""

    def __new__(cls, python_callable, multiple_outputs, **kwargs):
        """Alias ``@task`` to ``@task.python``."""
        return python_task(python_callable, multiple_outputs, **kwargs)
