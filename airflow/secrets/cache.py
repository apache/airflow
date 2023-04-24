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

    __manager: multiprocessing.managers.SyncManager | None = None
    _cache: dict[str, _CacheValue] | None = None
    _ttl: datetime.timedelta

    class NotPresent(Exception):
        """Raised when a key is not present in the cache"""

        ...

    class _CacheValue:
        def __init__(self, value: str | None) -> None:
            self.value = value
            self.date = datetime.datetime.utcnow()

        def is_expired(self, ttl: datetime.timedelta) -> bool:
            return datetime.datetime.utcnow() - self.date > ttl

    @classmethod
    def init(cls):
        """Initializes the cache, provided the configuration allows it. Safe to call several times."""
        if cls._cache is not None:
            return
        use_cache = conf.getboolean(section="secrets", key="use_cache", fallback=False)
        if not use_cache:
            return
        if cls.__manager is None:
            # it is not really necessary to save the manager, but doing so allows to reuse it between tests,
            # making them run a lot faster because this operation takes ~300ms each time
            cls.__manager = multiprocessing.Manager()
        cls._cache = cls.__manager.dict()
        ttl_seconds = conf.getint(section="secrets", key="cache_ttl_seconds", fallback=15 * 60)
        cls._ttl = datetime.timedelta(seconds=ttl_seconds)

    @classmethod
    def reset(cls):
        """For test purposes only"""
        cls._cache = None

    @classmethod
    def get_variable(cls, key: str) -> str | None:
        """
        Tries to get the value associated with the key from the cache

        :return: The saved value (which can be None) if present in cache and not expired,
            a NotPresent exception otherwise.
        """
        if cls._cache is None:
            # using an exception for misses allow to meaningfully cache None values
            raise cls.NotPresent

        val = cls._cache.get(key)
        if val and not val.is_expired(cls._ttl):
            return val.value
        raise cls.NotPresent

    @classmethod
    def save_variable(cls, key: str, value: str | None):
        """Saves the value for that key in the cache, if initialized"""
        if cls._cache is not None:
            cls._cache[key] = cls._CacheValue(value)

    @classmethod
    def invalidate_key(cls, key: str):
        """Invalidates (actually removes) the value stored in the cache for that key."""
        if cls._cache is not None:
            cls._cache.pop(key, None)  # second arg ensures no exception if key is absent
