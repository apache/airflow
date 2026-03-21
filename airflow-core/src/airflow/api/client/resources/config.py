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
"""Config resource sub-client for the local REST client."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.api_fastapi.core_api.datamodels.config import Config

if TYPE_CHECKING:
    import httpx


class ConfigClient:
    """Client for ``/api/v2/config`` endpoints (read-only)."""

    def __init__(self, http: httpx.Client) -> None:
        self._http = http

    def get(self, *, section: str | None = None) -> Config:
        """Get the full Airflow configuration, optionally filtered by section."""
        params: dict[str, str] = {}
        if section is not None:
            params["section"] = section
        resp = self._http.get("/api/v2/config", params=params)
        resp.raise_for_status()
        return Config.model_validate(resp.json())

    def get_value(self, section: str, option: str) -> Config:
        """Get a single configuration option value."""
        resp = self._http.get(f"/api/v2/config/section/{section}/option/{option}")
        resp.raise_for_status()
        return Config.model_validate(resp.json())
