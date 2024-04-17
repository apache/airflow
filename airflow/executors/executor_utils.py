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

    def __init__(self, module_path, alias=None):
        self.module_path = module_path
        self.alias = alias
        self.set_connector_source()

    def set_connector_source(self):
        if self.alias in CORE_EXECUTOR_NAMES:
            self.connector_source = ConnectorSource.CORE
        # If there is only one dot, then this is likely a plugin. This is the best we can do
        # to determine.
        elif self.module_path.count(".") == 1:
            self.log.debug(
                "The executor name looks like the plugin path (executor_name=%s) due to having "
                "just two period delimited parts. Treating executor as a plugin",
                self.module_path,
            )
            self.connector_source = ConnectorSource.PLUGIN
        # Executor must be a module
        else:
            self.connector_source = ConnectorSource.CUSTOM_PATH

    def __repr__(self):
        """Implement repr."""
        if self.alias in CORE_EXECUTOR_NAMES:
            return self.alias
        return f"{self.alias}:{self.module_path}" if self.alias else f"{self.module_path}"

    def __eq__(self, other):
        """Implement eq."""
        if (
            self.alias == other.alias
            and self.module_path == other.module_path
            and self.connector_source == other.connector_source
        ):
            return True
        else:
            return False

    def __hash__(self):
        """Implement hash."""
        return hash(self.__repr__())
