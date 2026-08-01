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
"""Objects relating to sourcing connections from environment variables."""

from __future__ import annotations

import os
import re

from airflow.secrets import BaseSecretsBackend

CONN_ENV_PREFIX = "AIRFLOW_CONN_"
VAR_ENV_PREFIX = "AIRFLOW_VAR_"

# Separator between the team name and the secret id in a team namespaced
# environment variable name: AIRFLOW_CONN__<TEAM>___<ID>.
TEAM_SEP = "___"

# What ``airflow teams create`` accepts as a team name, so what a team
# namespace can actually be spelled with.
# Kept in sync with ``airflow.cli.commands.team_command``. Underscores are excluded
# there so that this namespace stays unambiguous to read back.
_TEAM_NAME_CHARS = r"[a-zA-Z0-9-]{3,50}"


class EnvironmentVariablesBackend(BaseSecretsBackend):
    """Retrieves Connection object and Variable from environment variable."""

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        if team_name and (
            team_var := os.environ.get(f"{CONN_ENV_PREFIX}_{team_name.upper()}___" + conn_id.upper())
        ):
            # Format to set a team specific connection: AIRFLOW_CONN__<TEAM_ID>___<CONN_ID>
            return team_var

        if self._names_a_team_namespace(conn_id):
            return None

        return os.environ.get(CONN_ENV_PREFIX + conn_id.upper())

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable from Environment Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if team_name and (
            team_var := os.environ.get(f"{VAR_ENV_PREFIX}_{team_name.upper()}___" + key.upper())
        ):
            # Format to set a team specific variable: AIRFLOW_VAR__<TEAM_ID>___<VAR_KEY>
            return team_var

        if self._names_a_team_namespace(key):
            return None

        return os.environ.get(VAR_ENV_PREFIX + key.upper())

    @staticmethod
    def _names_a_team_namespace(secret_id: str) -> bool:
        """
        Whether ``secret_id`` spells out a team namespaced environment variable name.

        A team specific secret lives in the ``_<TEAM_NAME>___<SECRET_ID>`` namespace of the
        environment. An id of that shape makes the team agnostic lookup -- which prepends only
        ``AIRFLOW_CONN_`` / ``AIRFLOW_VAR_`` -- land inside some team's namespace, so that
        lookup is refused for such an id.

        The match is a single unambiguous one because team names cannot contain an underscore
        (``TEAM_NAME_PATTERN``). Without that rule a team name could itself span the ``___``
        separator, leaving ``_a___b___c`` readable as team ``a`` with id ``b___c`` *or* team
        ``a___b`` with id ``c``, with nothing in the string to choose between them -- which is
        what previously forced this check to try every split and to test each candidate
        against the team name rule.

        An id whose leading segment could not be a team name is still resolved: it names no
        team's namespace, so refusing it would block a legitimate team agnostic secret.
        """
        return re.fullmatch(rf"_{_TEAM_NAME_CHARS}{TEAM_SEP}.+", secret_id) is not None
