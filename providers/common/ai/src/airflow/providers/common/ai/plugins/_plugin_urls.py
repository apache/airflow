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
"""URL helpers shared by this provider's React plugins (``hitl_review``, ``model_panel``)."""

from __future__ import annotations

from urllib.parse import urlparse

from airflow.providers.common.compat.sdk import conf


def get_base_url_path(path: str) -> str:
    """Construct URL path with webserver base_url prefix for non-root deployments."""
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        base_path = urlparse(base_url).path
    else:
        base_path = base_url
    base_path = base_path.rstrip("/")
    return base_path + path


def get_bundle_url(plugin_prefix: str, bundle_filename: str) -> str:
    """
    Return the bundle URL for a plugin's React bundle.

    Only the path is used, never the origin in api.base_url: the host UI resolves it against the
    page it was loaded from, so the bundle still loads when the browser reaches Airflow through
    a different origin than base_url names, such as a local proxy.
    """
    return get_base_url_path(f"{plugin_prefix}/static/{bundle_filename}")
