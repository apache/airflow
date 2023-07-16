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

from dataclasses import dataclass

from airflow.configuration import conf
from airflow.executors.executor_constants import CELERY_EXECUTOR, CELERY_KUBERNETES_EXECUTOR


@dataclass
class CeleryCommandCompatibility:
    """
    Class for storing information about celery command compatibility.

    :param import_error: True if celery module is not installed
    :param wrong_executor: True if executor is not CeleryExecutor or CeleryKubernetesExecutor derived class
    :param message: Message to be displayed to the user
    """

    import_error: bool
    wrong_executor: bool
    message: str

    @property
    def is_compatible(self):
        return not any([self.import_error, self.wrong_executor])


def check_celery_executor_compatibility() -> CeleryCommandCompatibility:
    executor = conf.get("core", "EXECUTOR")
    celery_command_compatibility = CeleryCommandCompatibility(
        import_error=False, wrong_executor=False, message=""
    )
    from airflow.executors.executor_loader import ExecutorLoader

    if executor not in (CELERY_EXECUTOR, CELERY_KUBERNETES_EXECUTOR):
        classes: list[type] = []
        try:
            from airflow.providers.celery.executors.celery_executor import CeleryExecutor

            executor_cls, _ = ExecutorLoader.import_executor_cls(executor)

            classes.append(CeleryExecutor)
        except ImportError:
            celery_command_compatibility.import_error = True
            celery_command_compatibility.message = (
                "The celery subcommand requires that you pip install the celery module. "
                "To do it, run: pip install 'apache-airflow[celery]'"
            )
            return celery_command_compatibility
        try:
            from airflow.providers.celery.executors.celery_kubernetes_executor import CeleryKubernetesExecutor

            classes.append(CeleryKubernetesExecutor)
        except ImportError:
            pass
        if not issubclass(executor_cls, tuple(classes)):
            celery_command_compatibility.wrong_executor = True
            celery_command_compatibility.message = (
                f"The celery subcommand works only with CeleryExecutor, CeleryKubernetesExecutor and "
                f"executors derived from them, your current executor: {executor}, subclassed from: "
                f'{", ".join([base_cls.__qualname__ for base_cls in executor_cls.__bases__])}'
            )
    return celery_command_compatibility
