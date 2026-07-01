#
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
"""Abstract base class for Teradata authentication mechanisms."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    try:
        from airflow.sdk import Connection
    except ImportError:
        from airflow.models.connection import Connection  # type: ignore


class TeradataAuthMechanism(ABC):
    """Abstract base class for Teradata authentication mechanisms."""

    @property
    @abstractmethod
    def mechanism_name(self) -> str:
        """Return the machine-readable mechanism identifier."""

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Return the human-readable mechanism name."""

    @abstractmethod
    def get_connection_config(
        self,
        connection: Connection,
        base_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Transform Airflow Connection into teradatasql driver configuration."""

    @abstractmethod
    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate mechanism-specific configuration parameters."""
