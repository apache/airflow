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
from typing import TYPE_CHECKING

from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.exceptions import AirflowConfigException, UnknownExecutorException
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CELERY_KUBERNETES_EXECUTOR,
    CORE_EXECUTOR_NAMES,
    DEBUG_EXECUTOR,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
    LOCAL_KUBERNETES_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
    ConnectorSource,
)
from airflow.executors.executor_utils import ExecutorName
from airflow.utils.module_loading import import_string

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from airflow.executors.base_executor import BaseExecutor


# Used to lookup an ExecutorName via a string alias or module path. An
# executor may have both so we need two lookup dicts.
_alias_to_executors: dict[str, ExecutorName] = {}
_module_to_executors: dict[str, ExecutorName] = {}
_classname_to_executors: dict[str, ExecutorName] = {}
# Used to cache the computed ExecutorNames so that we don't need to read/parse config more than once
_executor_names: list[ExecutorName] = []
# Used to cache executors so that we don't construct executor objects unnecessarily
_loaded_executors: dict[ExecutorName, BaseExecutor] = {}


class ExecutorLoader:
    """Keeps constants for all the currently available executors."""

    executors = {
        LOCAL_EXECUTOR: "airflow.executors.local_executor.LocalExecutor",
        LOCAL_KUBERNETES_EXECUTOR: "airflow.providers.cncf.kubernetes."
        "executors.local_kubernetes_executor.LocalKubernetesExecutor",
        SEQUENTIAL_EXECUTOR: "airflow.executors.sequential_executor.SequentialExecutor",
        CELERY_EXECUTOR: "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
        CELERY_KUBERNETES_EXECUTOR: "airflow.providers.celery."
        "executors.celery_kubernetes_executor.CeleryKubernetesExecutor",
        KUBERNETES_EXECUTOR: "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor.KubernetesExecutor",
        DEBUG_EXECUTOR: "airflow.executors.debug_executor.DebugExecutor",
    }

    @classmethod
    def _get_executor_names(cls) -> list[ExecutorName]:
        """
        Return the executor names from Airflow configuration.

        :return: List of executor names from Airflow configuration
        """
        from airflow.configuration import conf

        if _executor_names:
            return _executor_names

        executor_names_raw = conf.get_mandatory_list_value("core", "EXECUTOR")

        executor_names = []
        for name in executor_names_raw:
            if len(split_name := name.split(":")) == 1:
                name = split_name[0]
                # Check if this is an alias for a core airflow executor, module
                # paths won't be provided by the user in that case.
                if core_executor_module := cls.executors.get(name):
                    executor_names.append(ExecutorName(alias=name, module_path=core_executor_module))
                # Only a module path or plugin name was provided
                else:
                    executor_names.append(ExecutorName(alias=None, module_path=name))
            # An alias was provided with the module path
            elif len(split_name) == 2:
                # Ensure the user is not trying to override the existing aliases of any of the core
                # executors by providing an alias along with the existing core airflow executor alias
                # (e.g. my_local_exec_alias:LocalExecutor). Allowing this makes things unnecessarily
                # complicated. Multiple Executors of the same type will be supported by a future multitenancy
                # AIP.
                # The module component should always be a module or plugin path.
                module_path = split_name[1]
                if not module_path or module_path in CORE_EXECUTOR_NAMES or "." not in module_path:
                    raise AirflowConfigException(
                        "Incorrectly formatted executor configuration. Second portion of an executor "
                        f"configuration must be a module path or plugin but received: {module_path}"
                    )
                else:
                    executor_names.append(ExecutorName(alias=split_name[0], module_path=split_name[1]))
            else:
                raise AirflowConfigException(f"Incorrectly formatted executor configuration: {name}")

        # As of now, we do not allow duplicate executors.
        # Add all module paths/plugin names to a set, since the actual code is what is unique
        unique_modules = set([exec_name.module_path for exec_name in executor_names])
        if len(unique_modules) < len(executor_names):
            msg = (
                "At least one executor was configured twice. Duplicate executors are not yet supported. "
                "Please check your configuration again to correct the issue."
            )
            raise AirflowConfigException(msg)

        # Populate some mappings for fast future lookups
        for executor_name in executor_names:
            # Executors will not always have aliases
            if executor_name.alias:
                _alias_to_executors[executor_name.alias] = executor_name
            # All executors will have a module path
            _module_to_executors[executor_name.module_path] = executor_name
            _classname_to_executors[executor_name.module_path.split(".")[-1]] = executor_name
            # Cache the executor names, so the logic of this method only runs once
            _executor_names.append(executor_name)

        return executor_names

    @classmethod
    def get_executor_names(cls) -> list[ExecutorName]:
        """
        Return the executor names from Airflow configuration.

        :return: List of executor names from Airflow configuration
        """
        return cls._get_executor_names()

    @classmethod
    def get_default_executor_name(cls) -> ExecutorName:
        """
        Return the default executor name from Airflow configuration.

        :return: executor name from Airflow configuration
        """
        # The default executor is the first configured executor in the list
        return cls._get_executor_names()[0]

    @classmethod
    def get_default_executor(cls) -> BaseExecutor:
        """Create a new instance of the configured executor if none exists and returns it."""
        default_executor = cls.load_executor(cls.get_default_executor_name())

        return default_executor

    @classmethod
    def set_default_executor(cls, executor: BaseExecutor) -> None:
        """
        Externally set an executor to be the default.

        This is used in rare cases such as dag.test which allows, as a user convenience, to provide
        the executor by cli/argument instead of Airflow configuration
        """
        exec_class_name = executor.__class__.__qualname__
        exec_name = ExecutorName(f"{executor.__module__}.{exec_class_name}")

        _module_to_executors[exec_name.module_path] = exec_name
        _classname_to_executors[exec_class_name] = exec_name
        _executor_names.insert(0, exec_name)
        _loaded_executors[exec_name] = executor

    @classmethod
    def init_executors(cls) -> list[BaseExecutor]:
        """Create a new instance of all configured executors if not cached already."""
        executor_names = cls._get_executor_names()
        loaded_executors = []
        for executor_name in executor_names:
            loaded_executor = cls.load_executor(executor_name.module_path)
            if executor_name.alias:
                cls.executors[executor_name.alias] = executor_name.module_path
            else:
                cls.executors[loaded_executor.__class__.__name__] = executor_name.module_path

            loaded_executors.append(loaded_executor)

        return loaded_executors

    @classmethod
    def lookup_executor_name_by_str(cls, executor_name_str: str) -> ExecutorName:
        # lookup the executor by alias first, if not check if we're given a module path
        if executor_name := _alias_to_executors.get(executor_name_str):
            return executor_name
        elif executor_name := _module_to_executors.get(executor_name_str):
            return executor_name
        elif executor_name := _classname_to_executors.get(executor_name_str):
            return executor_name
        else:
            raise UnknownExecutorException(f"Unknown executor being loaded: {executor_name_str}")

    @classmethod
    def load_executor(cls, executor_name: ExecutorName | str | None) -> BaseExecutor:
        """
        Load the executor.

        This supports the following formats:
        * by executor name for core executor
        * by ``{plugin_name}.{class_name}`` for executor from plugins
        * by import path
        * by class name of the Executor
        * by ExecutorName object specification

        :return: an instance of executor class via executor_name
        """
        if not executor_name:
            _executor_name = cls.get_default_executor_name()
        elif isinstance(executor_name, str):
            _executor_name = cls.lookup_executor_name_by_str(executor_name)
        else:
            _executor_name = executor_name

        # Check if the executor has been previously loaded. Avoid constructing a new object
        if _executor_name in _loaded_executors:
            return _loaded_executors[_executor_name]

        try:
            if _executor_name.alias == CELERY_KUBERNETES_EXECUTOR:
                executor = cls.__load_celery_kubernetes_executor()
            elif _executor_name.alias == LOCAL_KUBERNETES_EXECUTOR:
                executor = cls.__load_local_kubernetes_executor()
            else:
                executor_cls, import_source = cls.import_executor_cls(_executor_name)
                log.debug("Loading executor %s from %s", _executor_name, import_source.value)
                executor = executor_cls()

        except ImportError as e:
            log.error(e)
            raise AirflowConfigException(
                f'The module/attribute could not be loaded. Please check "executor" key in "core" section. '
                f'Current value: "{_executor_name}".'
            )
        log.info("Loaded executor: %s", _executor_name)

        # Store the executor name we've built for this executor in the
        # instance. This makes it easier for the Scheduler, Backfill, etc to
        # know how we refer to this executor.
        executor.name = _executor_name
        # Cache this executor by name here, so we can look it up later if it is
        # requested again, and not have to construct a new object
        _loaded_executors[_executor_name] = executor

        return executor

    @classmethod
    def import_executor_cls(
        cls, executor_name: ExecutorName, validate: bool = True
    ) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Import the executor class.

        Supports the same formats as ExecutorLoader.load_executor.

        :param executor_name: Name of core executor or module path to provider provided as a plugin.
        :param validate: Whether or not to validate the executor before returning

        :return: executor class via executor_name and executor import source
        """

        def _import_and_validate(path: str) -> type[BaseExecutor]:
            executor = import_string(path)
            if validate:
                cls.validate_database_executor_compatibility(executor)
            return executor

        if executor_name.connector_source == ConnectorSource.PLUGIN:
            with suppress(ImportError, AttributeError):
                # Load plugins here for executors as at that time the plugins might not have been
                # initialized yet
                from airflow import plugins_manager

                plugins_manager.integrate_executor_plugins()
                return (
                    _import_and_validate(f"airflow.executors.{executor_name.module_path}"),
                    ConnectorSource.PLUGIN,
                )
        return _import_and_validate(executor_name.module_path), executor_name.connector_source

    @classmethod
    def import_default_executor_cls(cls, validate: bool = True) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Import the default executor class.

        :param validate: Whether or not to validate the executor before returning

        :return: executor class and executor import source
        """
        executor_name = cls.get_default_executor_name()
        executor, source = cls.import_executor_cls(executor_name, validate=validate)
        return executor, source

    @classmethod
    @functools.lru_cache(maxsize=None)
    def validate_database_executor_compatibility(cls, executor: type[BaseExecutor]) -> None:
        """
        Validate database and executor compatibility.

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

        if InternalApiConfig.get_use_internal_api():
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
