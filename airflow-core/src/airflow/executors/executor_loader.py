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

import os
from collections import defaultdict
from typing import TYPE_CHECKING

import structlog

from airflow._shared.module_loading import import_string
from airflow.exceptions import AirflowConfigException, UnknownExecutorException
from airflow.executors.executor_constants import (
    CELERY_EXECUTOR,
    CORE_EXECUTOR_NAMES,
    KUBERNETES_EXECUTOR,
    LOCAL_EXECUTOR,
    ConnectorSource,
)
from airflow.executors.executor_utils import ExecutorName
from airflow.models.team import Team

log = structlog.get_logger(__name__)

if TYPE_CHECKING:
    from airflow.executors.base_executor import BaseExecutor


# Used to lookup an ExecutorName via a string alias or module path. An
# executor may have both so we need two lookup dicts.
_alias_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(dict)
_module_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(dict)
_classname_to_executors_per_team: dict[str | None, dict[str, ExecutorName]] = defaultdict(dict)
# Used to lookup an ExecutorName via the team id.
_team_name_to_executors: dict[str | None, list[ExecutorName]] = defaultdict(list)
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
    def _get_executor_names(cls, validate_teams: bool = True) -> list[ExecutorName]:
        """
        Return the executor names from Airflow configuration.

        :param validate_teams: Whether to validate that team names exist in database
        :return: List of executor names from Airflow configuration
        """
        if _executor_names:
            return _executor_names

        all_executor_names: list[tuple[str | None, list[str]]] = cls._get_team_executor_configs(
            validate_teams=validate_teams
        )

        executor_names: list[ExecutorName] = []
        for team_name, executor_names_config in all_executor_names:
            executor_names_per_team = []
            for executor_name_str in executor_names_config:
                if len(split_name := executor_name_str.split(":")) == 1:
                    name = split_name[0]
                    # Check if this is an alias for a core airflow executor, module
                    # paths won't be provided by the user in that case.
                    if core_executor_module := cls.executors.get(name):
                        executor_names_per_team.append(
                            ExecutorName(module_path=core_executor_module, alias=name, team_name=team_name)
                        )
                    # A module path was provided
                    else:
                        executor_names_per_team.append(
                            ExecutorName(alias=None, module_path=name, team_name=team_name)
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
                        ExecutorName(alias=split_name[0], module_path=split_name[1], team_name=team_name)
                    )
                else:
                    raise AirflowConfigException(
                        f"Incorrectly formatted executor configuration: {executor_name_str}"
                    )

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
                _alias_to_executors_per_team[executor_name.team_name][executor_name.alias] = executor_name
            # All executors will have a team name. It _may_ be None, for now that means it is a system level executor
            _team_name_to_executors[executor_name.team_name].append(executor_name)
            # All executors will have a module path
            _module_to_executors_per_team[executor_name.team_name][executor_name.module_path] = executor_name
            _classname_to_executors_per_team[executor_name.team_name][
                executor_name.module_path.split(".")[-1]
            ] = executor_name
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
    def _validate_teams_exist_in_database(cls, team_names: set[str]) -> None:
        """
        Validate that all specified team names exist in the database.

        :param team_names: Set of team names to validate
        :raises AirflowConfigException: If any team names don't exist in the database
        """
        if not team_names:
            return

        existing_teams = Team.get_all_team_names()

        missing_teams = team_names - existing_teams

        if missing_teams:
            missing_teams_list = sorted(missing_teams)
            missing_teams_str = ", ".join(missing_teams_list)

            raise AirflowConfigException(
                f"One or more teams specified in executor configuration do not exist in database: {missing_teams_str}. "
                "Please create these teams first or remove them from executor configuration."
            )

    @classmethod
    def _get_team_executor_configs(cls, validate_teams: bool = True) -> list[tuple[str | None, list[str]]]:
        """
        Return a list of executor configs to be loaded.

        Each tuple contains the team id as the first element and the second element is the executor config
        for that team (a list of executor names/modules/aliases).

        :param validate_teams: Whether to validate that team names exist in database
        """
        from airflow.configuration import conf

        executor_config = conf.get_mandatory_value("core", "executor")
        if not executor_config:
            raise AirflowConfigException(
                "The 'executor' key in the 'core' section of the configuration is mandatory and cannot be empty"
            )
        configs: list[tuple[str | None, list[str]]] = []
        seen_teams: set[str | None] = set()

        # The executor_config can look like a few things. One is just a single executor name, such as
        # "CeleryExecutor". Or a list of executors, such as "CeleryExecutor,KubernetesExecutor,module.path.to.executor".
        # In these cases these are all executors that are available to all teams, with the first one being the
        # default executor, as usual. The config can also look like a list of executors, per team, with the team name
        # prefixing each list of executors separated by a equal sign and then each team list separated by a
        # semi-colon.
        # "LocalExecutor;team1=CeleryExecutor;team2=KubernetesExecutor,module.path.to.executor".
        for team_executor_config in executor_config.split(";"):
            # The first item in the list may not have a team id (either empty string before the equal
            # sign or no equal sign at all), which means it is a global executor config.
            if "=" not in team_executor_config or team_executor_config.startswith("="):
                team_name = None
                executor_names = team_executor_config.strip("=")
            else:
                cls.block_use_of_multi_team()
                if conf.getboolean("core", "multi_team", fallback=False):
                    team_name, executor_names = team_executor_config.split("=")
                else:
                    log.warning(
                        "The 'multi_team' config is not enabled, but team executors were configured. "
                        "The following team executor config will be ignored: %s",
                        team_executor_config,
                    )
                    continue

            # Check for duplicate team names
            if team_name in seen_teams:
                raise AirflowConfigException(
                    f"Team '{team_name}' appears more than once in executor configuration. "
                    f"Each team can only be specified once in the executor config."
                )
            seen_teams.add(team_name)

            # Split by comma to get the individual executor names and strip spaces off of them
            configs.append((team_name, [name.strip() for name in executor_names.split(",")]))

        # Validate that at least one global executor exists
        has_global_executor = any(team_name is None for team_name, _ in configs)
        if not has_global_executor:
            raise AirflowConfigException(
                "At least one global executor must be configured. Current configuration only contains "
                "team-based executors. Please add a global executor configuration (e.g., "
                "'CeleryExecutor;team1=LocalExecutor' instead of 'team1=CeleryExecutor;team2=LocalExecutor')."
            )

        # Validate that all team names exist in the database (excluding None for global configs)
        team_names_to_validate = {team_name for team_name in seen_teams if team_name is not None}
        if team_names_to_validate and validate_teams:
            cls._validate_teams_exist_in_database(team_names_to_validate)

        return configs

    @classmethod
    def get_executor_names(cls, validate_teams: bool = True) -> list[ExecutorName]:
        """
        Return the executor names from Airflow configuration.

        :param validate_teams: Whether to validate that team names exist in database
        :return: List of executor names from Airflow configuration
        """
        return cls._get_executor_names(validate_teams=validate_teams)

    @classmethod
    def get_default_executor_name(cls, team_name: str | None = None) -> ExecutorName:
        """
        Return the default executor name from Airflow configuration.

        :return: executor name from Airflow configuration
        """
        cls._get_executor_names()
        # The default executor is the first configured executor in the list
        return _team_name_to_executors[team_name][0]

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
    def lookup_executor_name_by_str(
        cls, executor_name_str: str, team_name: str | None = None
    ) -> ExecutorName:
        # lookup the executor by alias first, if not check if we're given a module path
        if (
            not _classname_to_executors_per_team
            or not _module_to_executors_per_team
            or not _alias_to_executors_per_team
        ):
            # if we haven't loaded the executors yet, such as directly calling load_executor
            cls._get_executor_names()

        if executor_name := _alias_to_executors_per_team.get(team_name, {}).get(executor_name_str):
            return executor_name
        if executor_name := _module_to_executors_per_team.get(team_name, {}).get(executor_name_str):
            return executor_name
        if executor_name := _classname_to_executors_per_team.get(team_name, {}).get(executor_name_str):
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
            if _executor_name.team_name:
                executor = executor_cls(team_name=_executor_name.team_name)
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
