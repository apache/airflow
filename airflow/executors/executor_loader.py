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
"""All executors."""
from typing import Callable, Dict, Optional

from airflow.executors.base_executor import BaseExecutor, BaseExecutorProtocol


class ExecutorLoader:
    """
    Keeps constants for all the currently available executors.
    """

    LOCAL_EXECUTOR = "LocalExecutor"
    SEQUENTIAL_EXECUTOR = "SequentialExecutor"
    CELERY_EXECUTOR = "CeleryExecutor"
    DASK_EXECUTOR = "DaskExecutor"
    KUBERNETES_EXECUTOR = "KubernetesExecutor"

    _default_executor: Optional[BaseExecutorProtocol] = None

    _all_executors: Dict[str, BaseExecutor] = dict()

    @classmethod
    def get_default_executor(cls) -> BaseExecutorProtocol:
        """Creates a new instance of the configured executor if none exists and returns it"""
        if cls._default_executor is not None:
            return cls._default_executor

        from airflow.configuration import conf
        executor_name = conf.get('core', 'EXECUTOR')
        cls._default_executor = cls._get_executor(executor_name)
        return cls._default_executor

    @classmethod
    def get_or_create_executor(cls, executor_name: str,
                               create_executor: Callable[[], BaseExecutor]) -> BaseExecutor:
        """Retrieves (and creates if needed) an executor with the name specified"""
        if executor_name in cls._all_executors:
            return cls._all_executors[executor_name]
        executor = create_executor()
        cls._all_executors[executor_name] = executor
        return executor

    @staticmethod
    def create_local_executor() -> BaseExecutor:
        """Creates LocalExecutor"""
        from airflow.executors.local_executor import LocalExecutor
        return LocalExecutor()

    @staticmethod
    def create_sequential_executor() -> BaseExecutor:
        """Creates SequentialExecutor"""
        from airflow.executors.sequential_executor import SequentialExecutor
        return SequentialExecutor()

    @staticmethod
    def create_celery_executor() -> BaseExecutor:
        """Creates CeleryExecutor"""
        from airflow.executors.celery_executor import CeleryExecutor
        return CeleryExecutor()

    @staticmethod
    def create_dask_executor() -> BaseExecutor:
        """Creates DaskExecutor"""
        from airflow.executors.dask_executor import DaskExecutor
        return DaskExecutor()

    @staticmethod
    def create_kubernetes_executor() -> BaseExecutor:
        """Creates KubernetesExecutor"""
        from airflow.executors.kubernetes_executor import KubernetesExecutor
        return KubernetesExecutor()

    @classmethod
    def _get_executor(cls, executor_name: str) -> BaseExecutor:
        """
        Creates a new instance of the named executor.
        In case the executor name is unknown in airflow,
        look for it in the plugins.
        """
        if executor_name == ExecutorLoader.LOCAL_EXECUTOR:
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=ExecutorLoader.create_local_executor)
        elif executor_name == ExecutorLoader.SEQUENTIAL_EXECUTOR:
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=ExecutorLoader.create_sequential_executor)
        elif executor_name == ExecutorLoader.CELERY_EXECUTOR:
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=ExecutorLoader.create_celery_executor)
        elif executor_name == ExecutorLoader.DASK_EXECUTOR:
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=ExecutorLoader.create_dask_executor)
        elif executor_name == ExecutorLoader.KUBERNETES_EXECUTOR:
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=ExecutorLoader.create_kubernetes_executor)
        else:
            # Load plugins here for executors as at that time the plugins might not have been initialized yet
            # TODO: verify the above and remove two lines below in case plugins are always initialized first
            from airflow import plugins_manager
            plugins_manager.integrate_executor_plugins()
            executor_path = executor_name.split('.')
            if len(executor_path) != 2:
                raise ValueError(f"Executor {executor_name} not supported: "
                                 f"please specify in format plugin_module.executor")
            if not executor_path[0] in globals():
                raise ValueError(f"Executor {executor_name} not supported")
            create_function = globals()[executor_path[0]].__dict__[executor_path[1]]
            executor = cls.get_or_create_executor(executor_name=executor_name,
                                                  create_executor=create_function)
        return executor
