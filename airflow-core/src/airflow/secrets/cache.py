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
from __future__ import annotations

from airflow._shared.secrets_backend.cache import SecretCache as _SecretCache
from airflow.configuration import conf


class SecretCache(_SecretCache):
    """Secret cache configured for the API server."""

    @classmethod
    def init(cls) -> None:
        """Initialize the cache when enabled by configuration."""
        if cls._cache is not None:
            return
        if conf.getboolean(section="secrets", key="use_cache", fallback=False):
            super()._initialize(
                ttl_seconds=conf.getint(section="secrets", key="cache_ttl_seconds", fallback=15 * 60)
            )
