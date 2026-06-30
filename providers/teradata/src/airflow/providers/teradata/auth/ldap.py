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
"""LDAP authentication mechanism for Teradata."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.teradata.auth.base import TeradataAuthMechanism

if TYPE_CHECKING:
    try:
        from airflow.sdk import Connection
    except ImportError:
        from airflow.models.connection import Connection  # type: ignore


class LDAPAuthMechanism(TeradataAuthMechanism):
    """LDAP authentication mechanism for Teradata."""

    @property
    def mechanism_name(self) -> str:
        """Return 'LDAP' as the mechanism identifier."""
        return "LDAP"

    @property
    def display_name(self) -> str:
        """Return human-readable name."""
        return "LDAP Authentication"

    def get_connection_config(
        self,
        connection: Connection,
        base_config: dict[str, Any],
    ) -> dict[str, Any]:
        """Build LDAP authentication configuration."""
        config = base_config.copy()
        config["logmech"] = "LDAP"
        return config

    def validate_config(self, config: dict[str, Any]) -> None:
        """Validate LDAP authentication configuration."""
        logmech = config.get("logmech")
        if logmech != "LDAP":
            raise ValueError(f"LDAP mechanism expects logmech='LDAP', got '{logmech}'")

        # logdata carries the credentials directly to the driver, so an explicit
        # user/password pair is not required in that case.
        if config.get("logdata"):
            return

        user = config.get("user")
        password = config.get("password")

        if not user:
            raise ValueError("LDAP authentication requires LDAP username (Login field)")
        if password is None:
            raise ValueError("LDAP authentication requires LDAP password (Password field)")
