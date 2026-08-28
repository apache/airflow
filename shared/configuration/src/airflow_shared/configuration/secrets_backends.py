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

import enum
from typing import TYPE_CHECKING, TypeVar

from .exceptions import AirflowConfigException

if TYPE_CHECKING:
    from .parser import AirflowConfigParser


_T = TypeVar("_T")


ENVIRONMENT_VARIABLE_BACKEND_PATH = "airflow.secrets.environment_variables.EnvironmentVariablesBackend"
EXECUTION_API_BACKEND_PATH = "airflow.sdk.execution_time.secrets.execution_api.ExecutionAPISecretsBackend"
METASTORE_BACKEND_PATH = "airflow.secrets.metastore.MetastoreBackend"


class Backend(enum.Enum):
    """Known secrets backends."""

    ENVIRONMENT_VARIABLE = "environment_variable"
    EXECUTION_API = "execution_api"
    CUSTOM = "custom"
    METASTORE = "metastore"

    @classmethod
    def from_path(cls, default_backend: str) -> Backend:
        if default_backend == ENVIRONMENT_VARIABLE_BACKEND_PATH:
            return cls.ENVIRONMENT_VARIABLE
        if default_backend == EXECUTION_API_BACKEND_PATH:
            return cls.EXECUTION_API
        if default_backend == METASTORE_BACKEND_PATH:
            return cls.METASTORE

        raise ValueError(f"Unknown module provided: {default_backend}")


def _get_secrets_backend_order(
    conf: AirflowConfigParser, required_backends: list[Backend], worker_mode: bool
) -> list[Backend]:
    search_section = "workers" if worker_mode else "secrets"
    invalid_backends = []
    backends_order = []
    for backend in conf.getlist(search_section, "backends_order", delimiter=","):
        try:
            backends_order.append(Backend(backend))
        except ValueError:
            invalid_backends.append(backend)

    if invalid_backends:
        raise AirflowConfigException(
            f"The configuration option [{search_section}]backends_order is misconfigured. "
            f"The following backend types are unsupported: {invalid_backends}",
        )

    # backend is in use but its missing from ordering
    if missing_backends := [b.value for b in required_backends if b not in backends_order]:
        raise AirflowConfigException(
            f"The configuration option [{search_section}]backends_order is misconfigured. "
            f"The following backend types are missing: {missing_backends}",
        )

    return backends_order


def sorted_backends(
    conf: AirflowConfigParser, backend_list: list[tuple[Backend, _T]], worker_mode: bool
) -> list[_T]:
    backends_order = _get_secrets_backend_order(conf, [b[0] for b in backend_list], worker_mode)
    return [b[1] for b in sorted(backend_list, key=lambda e: backends_order.index(e[0]))]
