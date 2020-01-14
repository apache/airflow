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
import importlib
from typing import Optional

from airflow.executors.base_executor import BaseExecutor


class ExecutorLoader:
    """
    Keeps constants for all the currently available executors.
    """

    LOCAL_EXECUTOR = "LocalExecutor"
    SEQUENTIAL_EXECUTOR = "SequentialExecutor"
    CELERY_EXECUTOR = "CeleryExecutor"
    DASK_EXECUTOR = "DaskExecutor"
    KUBERNETES_EXECUTOR = "KubernetesExecutor"
    DEBUG_EXECUTOR = "DebugExecutor"

    _default_executor: Optional[BaseExecutor] = None
    executors = {
        LOCAL_EXECUTOR: 'airflow.executors.local_executor',
        SEQUENTIAL_EXECUTOR: 'airflow.executors.sequential_executor',
        CELERY_EXECUTOR: 'airflow.executors.celery_executor',
        DASK_EXECUTOR: 'airflow.executors.dask_executor',
        KUBERNETES_EXECUTOR: 'airflow.executors.kubernetes_executor',
        DEBUG_EXECUTOR: 'airflow.executors.debug_executor'
    }

    @classmethod
    def get_default_executor(cls) -> BaseExecutor:
        """Creates a new instance of the configured executor if none exists and returns it"""
        if cls._default_executor is not None:
            return cls._default_executor

        from airflow.configuration import conf
        executor_name = conf.get('core', 'EXECUTOR')

        cls._default_executor = ExecutorLoader._get_executor(executor_name)

        from airflow import LoggingMixin
        log = LoggingMixin().log
        log.info("Using executor %s", executor_name)

        return cls._default_executor

    @classmethod
    def _get_executor(cls, executor_name: str) -> BaseExecutor:
        """
        Creates a new instance of the named executor.
        In case the executor name is unknown in airflow,
        look for it in the plugins
        """
        if executor_name in cls.executors:
            executor_module = importlib.import_module(cls.executors[executor_name])
            executor = getattr(executor_module, executor_name)
            return executor()
        else:
            # Load plugins here for executors as at that time the plugins might not have been initialized yet
            # TODO: verify the above and remove two lines below in case plugins are always initialized first
            from airflow import plugins_manager
            plugins_manager.integrate_executor_plugins()
            executor_path = executor_name.split('.')
            if len(executor_path) != 2:
                raise ValueError(f"Executor {executor_name} not supported: "
                                 f"please specify in format plugin_module.executor")
            if executor_path[0] not in globals():
                raise ValueError(f"Executor {executor_name} not supported")
            return globals()[executor_path[0]].__dict__[executor_path[1]]()
