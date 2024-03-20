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

from typing import TYPE_CHECKING

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from fsspec import AbstractFileSystem

GCS_TOKEN = "gcs.oauth2.token"
GCS_TOKEN_EXPIRES_AT_MS = "gcs.oauth2.token-expires-at"
GCS_PROJECT_ID = "gcs.project-id"
GCS_ACCESS = "gcs.access"
GCS_CONSISTENCY = "gcs.consistency"
GCS_CACHE_TIMEOUT = "gcs.cache-timeout"
GCS_REQUESTER_PAYS = "gcs.requester-pays"
GCS_SESSION_KWARGS = "gcs.session-kwargs"
GCS_ENDPOINT = "gcs.endpoint"
GCS_DEFAULT_LOCATION = "gcs.default-bucket-location"
GCS_VERSION_AWARE = "gcs.version-aware"


schemes = ["gs", "gcs"]


def get_fs(conn_id: str | None, storage_options: dict[str, str] | None = None) -> AbstractFileSystem:
    # https://gcsfs.readthedocs.io/en/latest/api.html#gcsfs.core.GCSFileSystem
    from gcsfs import GCSFileSystem

    if conn_id is None:
        return GCSFileSystem()

    g = GoogleBaseHook(gcp_conn_id=conn_id)

    options = {
        "project": g.project_id,
        "access": g.extras.get(GCS_ACCESS, "full_control"),
        "token": g._get_access_token(),
        "consistency": g.extras.get(GCS_CONSISTENCY, "none"),
        "cache_timeout": g.extras.get(GCS_CACHE_TIMEOUT),
        "requester_pays": g.extras.get(GCS_REQUESTER_PAYS, False),
        "session_kwargs": g.extras.get(GCS_SESSION_KWARGS, {}),
        "endpoint_url": g.extras.get(GCS_ENDPOINT),
        "default_location": g.extras.get(GCS_DEFAULT_LOCATION),
        "version_aware": g.extras.get(GCS_VERSION_AWARE, "false").lower() == "true",
    }
    options.update(storage_options or {})

    return GCSFileSystem(**options)
