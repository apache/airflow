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

from airflow._shared.secrets_backend.base import BaseSecretsBackend as _BaseSecretsBackend


class BaseSecretsBackend(_BaseSecretsBackend):
    """Base class for secrets backend with Core Connection as default."""

    def _get_connection_class(self) -> type:
        conn_class = getattr(self, "_connection_class", None)
        if conn_class is None:
            from airflow.models import Connection

            self._connection_class = Connection
            return Connection
        return conn_class


# Server side default secrets backend search path used by server components (scheduler, API server)
DEFAULT_SECRETS_SEARCH_PATH = [
    "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    "airflow.secrets.metastore.MetastoreBackend",
]
