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

import logging
from collections.abc import Iterable
from typing import TYPE_CHECKING

from airflow.exceptions import (
    AirflowLoadCliDefinitionsException,
    AirflowLoadProviderCliDefinitionException,
)
from airflow.providers_manager import ProvidersManager
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.cli.cli_config import CLICommand

log = logging.getLogger(__name__)


class CliDefinitionLoader:
    """
    CLI Definition Loader.

    Similar to `ExecutorLoader`, but designed for CLI definitions.
    Currently, only `AuthManager.get_cli_commands()` and `Executor.get_cli_commands()` are called and extended in `cli_parser`. By introducing this class, we can enable provider-level modules to register their own CLI commands dynamically.
    Additionally, this change can improve CLI performance by **3-4x**, as demonstrated in the [benchmark](https://github.com/apache/airflow/issues/46789).
    """

    # Currently only kubernetes provider has **provider module level** CLI commands
    # https://github.com/apache/airflow/issues/46978
    providers_cli_definitions: dict[str, str] = {
        "apache-airflow-providers-cncf-kubernetes": "airflow.providers.cncf.kubernetes.cli.definition.KUBERNETES_GROUP_COMMANDS",
    }
    # TODO: Further step, decuple the `get_cli_commands` method from `Executor` and `AuthManager`
    # PoC: https://github.com/apache/airflow/commit/2750c0a336d57f9044e458b3fb1b348562da4f35

    @classmethod
    def _load_provider_cli_definitions(
        self,
    ) -> tuple[list[CLICommand], list[AirflowLoadProviderCliDefinitionException]]:
        """Load provider module level CLI definitions based on ProvidersManager."""
        errors = []
        provider_cli_definitions = []
        for provider_name in ProvidersManager().providers.keys():
            if provider_name in self.providers_cli_definitions:
                module_name = self.providers_cli_definitions[provider_name]
                try:
                    provider_cli_definitions.extend(import_string(module_name))
                except Exception:
                    # don't raise exception here, since we still want to load other provider CLI commands
                    errors.append(
                        AirflowLoadProviderCliDefinitionException(
                            f"Failed to load CLI commands from provider: {provider_name}"
                        )
                    )
        return provider_cli_definitions, errors

    @classmethod
    def get_cli_commands(self) -> Iterable[CLICommand]:
        """Get CLI commands from Providers, Executors, and AuthManager."""
        cli_commands = []
        errors = []
        for loader in [
            self._load_provider_cli_definitions,
            # self._load_executor_cli_definitions,     # Executor and AuthManager CLI commands will be integrated in the next PR
            # self._load_auth_manager_cli_definitions,
        ]:
            commands, loader_errors = loader()
            cli_commands.extend(commands)
            errors.extend(loader_errors)

        yield from cli_commands
        # Raise all errors at once after yielding all commands
        if errors:
            raise AirflowLoadCliDefinitionsException(errors)
