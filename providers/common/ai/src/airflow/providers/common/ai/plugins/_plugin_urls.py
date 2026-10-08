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

    Uses an absolute URL when api.base_url is a full URL so the bundle loads
    correctly in Vite dev mode, where import() resolves relative to the script
    origin (5173) rather than the document origin (28080).
    """
    path = get_base_url_path(f"{plugin_prefix}/static/{bundle_filename}")
    base_url = conf.get("api", "base_url", fallback="/")
    if base_url.startswith(("http://", "https://")):
        parsed = urlparse(base_url)
        return f"{parsed.scheme}://{parsed.netloc}" + path
    return path
