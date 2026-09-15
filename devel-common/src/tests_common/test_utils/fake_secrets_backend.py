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

from airflow.secrets import BaseSecretsBackend

FAKE_BACKEND_PATH = f"{__name__}.FakeSecretsBackend"
FAKE_CONFIG_BACKEND_PATH = f"{__name__}.FakeConfigSecretsBackend"
FAKE_UNREACHABLE_BACKEND_PATH = f"{__name__}.FakeUnreachableSecretsBackend"


class FakeSecretsBackend(BaseSecretsBackend):
    """Secrets backend for tests that exercise backend configuration rather than lookups."""

    # Defaults deliberately differ from the values the tests pass in, so an assertion on an
    # attribute fails unless the configured kwarg — including an explicit JSON null — reached here.
    def __init__(
        self,
        connections_prefix: str | None = "/connections",
        variables_prefix: str | None = "/variables",
        use_ssl: bool = True,
        **kwargs,
    ) -> None:
        self.connections_prefix = connections_prefix
        self.variables_prefix = variables_prefix
        self.use_ssl = use_ssl
        self.kwargs = kwargs


class FakeConfigSecretsBackend(BaseSecretsBackend):
    """Secrets backend serving config values from a mapping passed as a backend kwarg."""

    def __init__(self, config_values: dict[str, str] | None = None, **kwargs) -> None:
        self.config_values = config_values or {}
        self.kwargs = kwargs

    def get_config(self, key: str) -> str | None:
        return self.config_values.get(key)


class FakeUnreachableSecretsBackend(BaseSecretsBackend):
    """Secrets backend whose config lookups fail, standing in for an unreachable backend."""

    def get_config(self, key: str) -> str | None:
        raise ConnectionError(f"Cannot reach the secrets backend to look up {key!r}")
