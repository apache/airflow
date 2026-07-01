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
"""Registry for Teradata authentication mechanisms."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

if TYPE_CHECKING:
    from airflow.providers.teradata.auth.base import TeradataAuthMechanism


class TeradataAuthRegistry:
    """Registry for Teradata authentication mechanisms."""

    _mechanisms: ClassVar[dict[str, TeradataAuthMechanism]] = {}

    @classmethod
    def register(cls, mechanism: TeradataAuthMechanism) -> None:
        """Register an authentication mechanism."""
        cls._mechanisms.setdefault(mechanism.mechanism_name, mechanism)

    @classmethod
    def get(cls, mechanism_name: str) -> TeradataAuthMechanism:
        """Get authentication mechanism by name."""
        if mechanism_name in cls._mechanisms:
            return cls._mechanisms[mechanism_name]
        supported = ", ".join(sorted(cls._mechanisms.keys()))
        raise ValueError(f"Unknown authentication mechanism: '{mechanism_name}'. Supported: {supported}")

    @classmethod
    def get_default(cls) -> TeradataAuthMechanism:
        """
        Get the default authentication mechanism (TD2).

        Returns:
            TD2 mechanism instance
        """
        return cls.get("TD2")

    @classmethod
    def available_mechanisms(cls) -> list[str]:
        """
        Get list of all registered mechanism names.

        Returns:
            List of mechanism names, sorted alphabetically
        """
        return sorted(cls._mechanisms.keys())
