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

from airflow.secrets import BaseSecretsBackend

CONN_ENV_PREFIX = "AIRFLOW_CONN_"
VAR_ENV_PREFIX = "AIRFLOW_VAR_"


class EnvironmentVariablesBackend(BaseSecretsBackend):
    """Retrieves Connection object and Variable from environment variable."""

    def get_conn_value(self, conn_id: str) -> str | None:
        return os.environ.get(CONN_ENV_PREFIX + conn_id.upper())

    def get_variable(self, key: str) -> str | None:
        """
        Get Airflow Variable from Environment Variable.

        :param key: Variable Key
        :return: Variable Value
        """
        return os.environ.get(VAR_ENV_PREFIX + key.upper())
