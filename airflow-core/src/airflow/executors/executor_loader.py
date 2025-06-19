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

import logging
import os
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowConfigException, UnknownExecutorException
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CORE_EXECUTOR_NAMES,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
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
# Used to lookup an ExecutorName via the team id.
_team_id_to_executors: dict[str | None, ExecutorName] = {}
_classname_to_executors: dict[str, ExecutorName] = {}
# Used to cache the computed ExecutorNames so that we don't need to read/parse config more than once
_executor_names: list[ExecutorName] = []


class ExecutorLoader:
    """Keeps constants for all the currently available executors."""

    executors = {
        LOCAL_EXECUTOR: "airflow.executors.local_executor.LocalExecutor",
        CELERY_EXECUTOR: "airflow.providers.celery.executors.celery_executor.CeleryExecutor",
        KUBERNETES_EXECUTOR: "airflow.providers.cncf.kubernetes."
        "executors.kubernetes_executor.KubernetesExecutor",
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

        all_executor_names: list[tuple[None | str, list[str]]] = [
            (None, conf.get_mandatory_list_value("core", "EXECUTOR"))
        ]
        all_executor_names.extend(cls._get_team_executor_configs())

        executor_names = []
        for team_id, executor_names_config in all_executor_names:
            executor_names_per_team = []
            for name in executor_names_config:
                if len(split_name := name.split(":")) == 1:
                    name = split_name[0]
                    # Check if this is an alias for a core airflow executor, module
                    # paths won't be provided by the user in that case.
                    if core_executor_module := cls.executors.get(name):
                        executor_names_per_team.append(
                            ExecutorName(module_path=core_executor_module, alias=name, team_id=team_id)
                        )
                    # A module path was provided
                    else:
                        executor_names_per_team.append(
                            ExecutorName(alias=None, module_path=name, team_id=team_id)
                        )
                # An alias was provided with the module path
                elif len(split_name) == 2:
                    # Ensure the user is not trying to override the existing aliases of any of the core
                    # executors by providing an alias along with the existing core airflow executor alias
                    # (e.g. my_local_exec_alias:LocalExecutor). Allowing this makes things unnecessarily
                    # complicated. Multiple Executors of the same type will be supported by a future
                    # multitenancy AIP.
                    # The module component should always be a module path.
                    module_path = split_name[1]
                    if not module_path or module_path in CORE_EXECUTOR_NAMES or "." not in module_path:
                        raise AirflowConfigException(
                            "Incorrectly formatted executor configuration. Second portion of an executor "
                            f"configuration must be a module path but received: {module_path}"
                        )
                    executor_names_per_team.append(
                        ExecutorName(alias=split_name[0], module_path=split_name[1], team_id=team_id)
                    )
                else:
                    raise AirflowConfigException(f"Incorrectly formatted executor configuration: {name}")

            # As of now, we do not allow duplicate executors (within teams).
            # Add all module paths to a set, since the actual code is what is unique
            unique_modules = set([exec_name.module_path for exec_name in executor_names_per_team])
            if len(unique_modules) < len(executor_names_per_team):
                msg = (
                    "At least one executor was configured twice. Duplicate executors are not yet supported.\n"
                    "Please check your configuration again to correct the issue."
                )
                raise AirflowConfigException(msg)

            executor_names.extend(executor_names_per_team)

        # Populate some mappings for fast future lookups
        for executor_name in executor_names:
            # Executors will not always have aliases
            if executor_name.alias:
                _alias_to_executors[executor_name.alias] = executor_name
            # All executors will have a team id. It _may_ be None, for now that means it is a system
            # level executor
            _team_id_to_executors[executor_name.team_id] = executor_name
            # All executors will have a module path
            _module_to_executors[executor_name.module_path] = executor_name
            _classname_to_executors[executor_name.module_path.split(".")[-1]] = executor_name
            # Cache the executor names, so the logic of this method only runs once
            _executor_names.append(executor_name)

        return executor_names

    @classmethod
    def block_use_of_multi_team(cls):
        """
        Raise an exception if the user tries to use multiple team based executors.

        Before the feature is complete we do not want users to accidentally configure this.
        This can be overridden by setting the AIRFLOW__DEV__MULTI_TEAM_MODE environment
        variable to "enabled"
        This check is built into a method so that it can be easily mocked in unit tests.
        """
        team_dev_mode: str | None = os.environ.get("AIRFLOW__DEV__MULTI_TEAM_MODE")
        if not team_dev_mode or team_dev_mode != "enabled":
            raise AirflowConfigException("Configuring multiple team based executors is not yet supported!")

    @classmethod
    def _get_team_executor_configs(cls) -> list[tuple[str, list[str]]]:
        """
        Return a list of executor configs to be loaded.

        Each tuple contains the team id as the first element and the second element is the executor config
        for that team (a list of executor names/modules/aliases).
        """
        from airflow.configuration import conf

        team_config = conf.get("core", "multi_team_config_files", fallback=None)
        configs = []
        if team_config:
            cls.block_use_of_multi_team()
            for team in team_config.split(","):
                (_, team_id) = team.split(":")
                configs.append((team_id, conf.get_mandatory_list_value("core", "executor", team_id=team_id)))
        return configs

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
    def init_executors(cls) -> list[BaseExecutor]:
        """Create a new instance of all configured executors if not cached already."""
        executor_names = cls._get_executor_names()
        loaded_executors = []
        for executor_name in executor_names:
            loaded_executor = cls.load_executor(executor_name)
            if executor_name.alias:
                cls.executors[executor_name.alias] = executor_name.module_path
            else:
                cls.executors[loaded_executor.__class__.__name__] = executor_name.module_path

            loaded_executors.append(loaded_executor)

        return loaded_executors

    @classmethod
    def lookup_executor_name_by_str(cls, executor_name_str: str) -> ExecutorName:
        # lookup the executor by alias first, if not check if we're given a module path
        if not _classname_to_executors or not _module_to_executors or not _alias_to_executors:
            # if we haven't loaded the executors yet, such as directly calling load_executor
            cls._get_executor_names()

        if executor_name := _alias_to_executors.get(executor_name_str):
            return executor_name
        if executor_name := _module_to_executors.get(executor_name_str):
            return executor_name
        if executor_name := _classname_to_executors.get(executor_name_str):
            return executor_name
        raise UnknownExecutorException(f"Unknown executor being loaded: {executor_name_str}")

    @classmethod
    def load_executor(cls, executor_name: ExecutorName | str | None) -> BaseExecutor:
        """
        Load the executor.

        This supports the following formats:
        * by executor name for core executor
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

        try:
            executor_cls, import_source = cls.import_executor_cls(_executor_name)
            log.debug("Loading executor %s from %s", _executor_name, import_source.value)
            if _executor_name.team_id:
                executor = executor_cls(team_id=_executor_name.team_id)
            else:
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

        return executor

    @classmethod
    def import_executor_cls(cls, executor_name: ExecutorName) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Import the executor class.

        Supports the same formats as ExecutorLoader.load_executor.

        :param executor_name: Name of core executor or module path to executor.

        :return: executor class via executor_name and executor import source
        """
        return import_string(executor_name.module_path), executor_name.connector_source

    @classmethod
    def import_default_executor_cls(cls) -> tuple[type[BaseExecutor], ConnectorSource]:
        """
        Import the default executor class.

        :return: executor class and executor import source
        """
        executor_name = cls.get_default_executor_name()
        executor, source = cls.import_executor_cls(executor_name)
        return executor, source
