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
from __future__ import annotations

import functools
import logging
import os
from contextlib import suppress
from enum import Enum, unique
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowConfigException
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    DASK_EXECUTOR,
    DEBUG_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
    LOCAL_KUBERNETES_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
)
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.executors.base_executor import BaseExecutor


@unique
class ConnectorSource(Enum):
    """Enum of supported executor import sources."""

    CORE = "core"
    PLUGIN = "plugin"
    CUSTOM_PATH = "custom path"


class ExecutorLoader:
    """Keeps constants for all the currently available executors."""

    _default_executor: BaseExecutor | None = None
    executors = {
        LOCAL_EXECUTOR: "airflow.executors.local_executor.LocalExecutor",
        LOCAL_KUBERNETES_EXECUTOR: "airflow.executors.local_kubernetes_executor.LocalKubernetesExecutor",
        SEQUENTIAL_EXECUTOR: "airflow.executors.sequential_executor.SequentialExecutor",
        CELERY_EXECUTOR: "airflow.executors.celery_executor.CeleryExecutor",
        CELERY_KUBERNETES_EXECUTOR: "airflow.executors.celery_kubernetes_executor.CeleryKubernetesExecutor",
        DASK_EXECUTOR: "airflow.executors.dask_executor.DaskExecutor",
        KUBERNETES_EXECUTOR: "airflow.executors.kubernetes_executor.KubernetesExecutor",
        DEBUG_EXECUTOR: "airflow.executors.debug_executor.DebugExecutor",
    }

    @classmethod
    def get_default_executor_name(cls) -> str:
        """Returns the default executor name from Airflow configuration.

        :return: executor name from Airflow configuration
        """
        from airflow.configuration import conf

        return conf.get_mandatory_value("core", "EXECUTOR")

    @classmethod
    def get_default_executor(cls) -> BaseExecutor:
        """Creates a new instance of the configured executor if none exists and returns it."""
        if cls._default_executor is not None:
            return cls._default_executor

        return cls.load_executor(cls.get_default_executor_name())

    @classmethod
    def load_executor(cls, executor_name: str) -> BaseExecutor:
        """
        Loads the executor.

        This supports the following formats:
        * by executor name for core executor
        * by ``{plugin_name}.{class_name}`` for executor from plugins
        * by import path.

        :return: an instance of executor class via executor_name
        """
        if executor_name == CELERY_KUBERNETES_EXECUTOR:
            return cls.__load_celery_kubernetes_executor()
        elif executor_name == LOCAL_KUBERNETES_EXECUTOR:
            return cls.__load_local_kubernetes_executor()

        try:
            executor_cls, import_source = cls.import_executor_cls(executor_name)
            log.debug("Loading executor %s from %s", executor_name, import_source.value)
        except ImportError as e:
            log.error(e)
            raise AirflowConfigException(
                f'The module/attribute could not be loaded. Please check "executor" key in "core" section. '
                f'Current value: "{executor_name}".'
            )
        log.info("Loaded executor: %s", executor_name)

        return executor_cls()

    @classmethod
    def import_executor_cls(cls, executor_name: str) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Imports the executor class.

        Supports the same formats as ExecutorLoader.load_executor.

        :return: executor class via executor_name and executor import source
        """

        def _import_and_validate(path: str) -> type[BaseExecutor]:
            executor = import_string(path)
            cls.validate_database_executor_compatibility(executor)
            return executor

        if executor_name in cls.executors:
            return _import_and_validate(cls.executors[executor_name]), ConnectorSource.CORE
        if executor_name.count(".") == 1:
            log.debug(
                "The executor name looks like the plugin path (executor_name=%s). Trying to import a "
                "executor from a plugin",
                executor_name,
            )
            with suppress(ImportError, AttributeError):
                # Load plugins here for executors as at that time the plugins might not have been
                # initialized yet
                from airflow import plugins_manager

                plugins_manager.integrate_executor_plugins()
                return _import_and_validate(f"airflow.executors.{executor_name}"), ConnectorSource.PLUGIN
        return _import_and_validate(executor_name), ConnectorSource.CUSTOM_PATH

    @classmethod
    def import_default_executor_cls(cls) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Imports the default executor class.

        :return: executor class and executor import source
        """
        executor_name = cls.get_default_executor_name()
        executor, source = cls.import_executor_cls(executor_name)
        return executor, source

    @classmethod
    @functools.lru_cache(maxsize=None)
    def validate_database_executor_compatibility(cls, executor: type[BaseExecutor]) -> None:
        """Validate database and executor compatibility.

        Most of the databases work universally, but SQLite can only work with
        single-threaded executors (e.g. Sequential).

        This is NOT done in ``airflow.configuration`` (when configuration is
        initialized) because loading the executor class is heavy work we want to
        avoid unless needed.
        """
        # Single threaded executors can run with any backend.
        if executor.is_single_threaded:
            return

        # This is set in tests when we want to be able to use SQLite.
        if os.environ.get("_AIRFLOW__SKIP_DATABASE_EXECUTOR_COMPATIBILITY_CHECK") == "1":
            return

        from airflow.settings import engine

        # SQLite only works with single threaded executors
        if engine.dialect.name == "sqlite":
            raise AirflowConfigException(f"error: cannot use SQLite with the {executor.__name__}")

    @classmethod
    def __load_celery_kubernetes_executor(cls) -> BaseExecutor:
        celery_executor = import_string(cls.executors[CELERY_EXECUTOR])()
        kubernetes_executor = import_string(cls.executors[KUBERNETES_EXECUTOR])()

        celery_kubernetes_executor_cls = import_string(cls.executors[CELERY_KUBERNETES_EXECUTOR])
        return celery_kubernetes_executor_cls(celery_executor, kubernetes_executor)

    @classmethod
    def __load_local_kubernetes_executor(cls) -> BaseExecutor:
        local_executor = import_string(cls.executors[LOCAL_EXECUTOR])()
        kubernetes_executor = import_string(cls.executors[KUBERNETES_EXECUTOR])()

        local_kubernetes_executor_cls = import_string(cls.executors[LOCAL_KUBERNETES_EXECUTOR])
        return local_kubernetes_executor_cls(local_executor, kubernetes_executor)


# This tuple is deprecated due to AIP-51 and is no longer used in core Airflow.
# TODO: Remove in Airflow 3.0
UNPICKLEABLE_EXECUTORS = (
    LOCAL_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
    DASK_EXECUTOR,
)
