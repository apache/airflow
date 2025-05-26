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

from airflow.executors.executor_constants import CORE_EXECUTOR_NAMES, ConnectorSource
from airflow.utils.log.logging_mixin import LoggingMixin


class ExecutorName(LoggingMixin):
    """Representation of an executor config/name."""

    def __init__(self, module_path: str, alias: str | None = None, team_id: str | None = None) -> None:
        self.module_path: str = module_path
        self.alias: str | None = alias
        self.team_id: str | None = team_id
        self.set_connector_source()

    def set_connector_source(self) -> None:
        if self.alias in CORE_EXECUTOR_NAMES:
            self.connector_source = ConnectorSource.CORE
        else:
            # Executor must be a module
            self.connector_source = ConnectorSource.CUSTOM_PATH

    def __repr__(self) -> str:
        """Implement repr."""
        if self.alias in CORE_EXECUTOR_NAMES:
            # This is a "core executor" we can refer to it by its known short name
            return f"{self.team_id if self.team_id else ''}:{self.alias}:"
        return f"{self.team_id if self.team_id else ''}:{self.alias if self.alias else ''}:{self.module_path}"

    def __eq__(self, other) -> bool:
        """Implement eq."""
        if (
            self.alias == other.alias
            and self.module_path == other.module_path
            and self.connector_source == other.connector_source
            and self.team_id == other.team_id
        ):
            return True
        return False

    def __hash__(self) -> int:
        """Implement hash."""
        return hash(self.__repr__())
