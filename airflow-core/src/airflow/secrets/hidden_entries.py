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

import os

from airflow._shared.configuration.parser import CUSTOM_BACKEND_CONFIG_KEYS
from airflow.configuration import conf


def get_hidden_entry_sources(env_prefix: str) -> list[str]:
    """
    Return the non-database sources that may define entries missing from a metadata database listing.

    :param env_prefix: Environment variable prefix used for this entity, e.g. ``AIRFLOW_CONN_``.
    :return: Human-readable names of the sources in use; empty if neither appears to be in use.
    """
    # Connections and variables may also come from environment variables or a
    # custom secrets backend. These sources can override database entries but
    # are not included by anything that enumerates database rows.
    has_env_vars = any(key.startswith(env_prefix) for key in os.environ)
    # Only check whether custom backends are *configured*, without instantiating them (which could
    # have side effects, e.g. opening a network connection to a Vault/AWS/GCP secrets service).
    # Reads the same (section, key) pairs _get_custom_secret_backend() uses, so a new backend
    # source only needs to be added in one place.
    has_secrets_backend = any(
        conf.get(section, key, fallback=None) for section, key in CUSTOM_BACKEND_CONFIG_KEYS.values()
    )

    sources = []
    if has_env_vars:
        sources.append(f"`{env_prefix}*` environment variables")
    if has_secrets_backend:
        sources.append("a configured secrets backend")
    return sources
