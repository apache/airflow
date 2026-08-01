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
        environment. An id of that shape therefore makes the team agnostic lookup -- which
        prepends only ``AIRFLOW_CONN_`` / ``AIRFLOW_VAR_`` -- land inside some team's namespace,
        so that lookup is refused for such an id.

        **The id is never attributed to a particular team**, because it cannot be: a team name
        may itself contain the ``___`` separator, so ``_a___b___c`` is both team ``a`` with id
        ``b___c`` and team ``a___b`` with id ``c``, and nothing in the string distinguishes them.
        Only the caller's own namespace is ever constructed, never parsed.

        This is why the check is not "does the id belong to some team other than the caller's".
        Comparing the id against the prefix the caller's team builds looks equivalent and is not:
        for a caller in team ``a`` the id ``_a___b___c`` starts with ``_A___``, yet the variable
        it resolves, ``AIRFLOW_CONN__A___B___C``, is team ``a___b``'s. Treating a prefix match as
        ownership hands one team the secrets of every team whose name extends it. Callers reach
        their own team's secrets with the bare id plus their team scope -- handled above, and safe
        because that path can only ever build the caller's own namespace -- so nothing legitimate
        needs a namespaced id here.
        """
        return re.fullmatch(r"_.+___.+", secret_id) is not None
