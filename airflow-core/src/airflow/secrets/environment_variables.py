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

from airflow.configuration import conf
from airflow.secrets import BaseSecretsBackend

CONN_ENV_PREFIX = "AIRFLOW_CONN_"
VAR_ENV_PREFIX = "AIRFLOW_VAR_"

# Separates the team name from the secret id in a team namespaced environment variable
# name: AIRFLOW_CONN__<TEAM>___<ID>.
TEAM_SEP = "___"


class EnvironmentVariablesBackend(BaseSecretsBackend):
    """Retrieves Connection object and Variable from environment variable."""

    @staticmethod
    def _names_a_team_namespace(secret_id: str) -> bool:
        """
        Whether ``secret_id`` spells out a team scoped secret name.

        Only checked in multi-team mode: ``team_name`` is never non-``None`` otherwise, so no
        team scoped variable can exist to collide with.
        """
        if not conf.getboolean("core", "multi_team", fallback=False):
            return False
        return TEAM_SEP in secret_id

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        if self._names_a_team_namespace(conn_id):
            return None

        if team_name and (
            team_var := os.environ.get(f"{CONN_ENV_PREFIX}_{team_name.upper()}___" + conn_id.upper())
        ):
            # Format to set a team specific connection: AIRFLOW_CONN__<TEAM_ID>___<CONN_ID>
            return team_var

        return os.environ.get(CONN_ENV_PREFIX + conn_id.upper())

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable from Environment Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if self._names_a_team_namespace(key):
            return None

        if team_name and (
            team_var := os.environ.get(f"{VAR_ENV_PREFIX}_{team_name.upper()}___" + key.upper())
        ):
            # Format to set a team specific variable: AIRFLOW_VAR__<TEAM_ID>___<VAR_KEY>
            return team_var

        return os.environ.get(VAR_ENV_PREFIX + key.upper())
