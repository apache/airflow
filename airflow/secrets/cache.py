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

import datetime
import multiprocessing

from airflow.configuration import conf


class SecretCache:
    """A static class to manage the global secret cache"""

    CACHED_NONE_VALUE = object()  # Sentinel object to distinguish between None and expired value

    _cache: dict[str, _CacheValue] | None = None
    _ttl: datetime.timedelta

    class _CacheValue:
        def __init__(self, value: str | None) -> None:
            if value is None:
                self.value = SecretCache.CACHED_NONE_VALUE
            else:
                self.value = value
            self.date = datetime.datetime.utcnow()

        def is_expired(self, ttl: datetime.timedelta) -> bool:
            return datetime.datetime.utcnow() - self.date > ttl

    @classmethod
    def init(cls):
        """initializes the cache, provided the configuration allows it. Safe to call several times."""
        if cls._cache is not None:
            return
        use_cache = conf.getboolean(section="secrets", key="use_cache", fallback=True)
        if not use_cache:
            return
        cls._cache = multiprocessing.Manager().dict()
        ttl_seconds = conf.getint(section="secrets", key="cache_ttl_seconds", fallback=15 * 60)
        cls._ttl = datetime.timedelta(seconds=ttl_seconds)

    @classmethod
    def get_variable(cls, key: str) -> str | None:
        """
        Tries to get the value associated with the key from the cache

        :return: None if the value was not in the cache or expired, or if the cache is disabled.
            The CACHED_NONE_VALUE sentinel is returned if None was the saved value.
        """
        if cls._cache is None:
            return None

        val = cls._cache.get(key)
        if val and not val.is_expired(cls._ttl):
            return val.value
        return None

    @classmethod
    def save_variable(cls, key: str, value: str | None):
        """saves the value for that key in the cache, if enabled"""
        cls.init()  # ensure initialization has been done
        if cls._cache is not None:
            cls._cache[key] = cls._CacheValue(value)

    @classmethod
    def invalidate_key(cls, key: str):
        """invalidates (actually removes) the value stored in the cache for that key."""
        if cls._cache is not None:
            cls._cache.pop(key)
