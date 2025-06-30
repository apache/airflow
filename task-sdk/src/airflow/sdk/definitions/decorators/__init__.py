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

from collections.abc import Callable

from airflow.providers_manager import ProvidersManager
from airflow.sdk.bases.decorator import TaskDecorator
from airflow.sdk.definitions.dag import dag
from airflow.sdk.definitions.decorators.condition import run_if, skip_if
from airflow.sdk.definitions.decorators.setup_teardown import setup_task, teardown_task
from airflow.sdk.definitions.decorators.task_group import task_group

# Please keep this in sync with the .pyi's __all__.
__all__ = [
    "TaskDecorator",
    "TaskDecoratorCollection",
    "dag",
    "task",
    "task_group",
    "setup",
    "teardown",
]


class TaskDecoratorCollection:
    """Implementation to provide the ``@task`` syntax."""

    run_if = staticmethod(run_if)
    skip_if = staticmethod(skip_if)

    def __getattr__(self, name: str) -> TaskDecorator:
        """Dynamically get provider-registered task decorators, e.g. ``@task.docker``."""
        if name.startswith("__"):
            raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")
        decorators = ProvidersManager().taskflow_decorators
        if name not in decorators:
            raise AttributeError(f"task decorator {name!r} not found")
        return decorators[name]

    def __call__(self, *args, **kwargs):
        """Alias '@task' to python dynamically."""
        return self.__getattr__("python")(*args, **kwargs)


task = TaskDecoratorCollection()
setup: Callable = setup_task
teardown: Callable = teardown_task
