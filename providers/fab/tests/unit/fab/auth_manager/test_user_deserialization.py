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

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest.mock import patch

import pytest
from cachetools import TTLCache

from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers.fab.auth_manager.models import User


@patch.object(FabAuthManager, "deserialize_user", autospec=True)
@pytest.mark.asyncio
async def test_deserialize_user_async_does_not_block_event_loop(deserialize_user):
    manager = FabAuthManager()
    user = User(id=123, active=True)
    payload = {"sub": "123"}
    event_loop_thread = threading.get_ident()
    progressed = threading.Event()

    def blocking_lookup(manager, token):
        assert threading.get_ident() != event_loop_thread
        assert progressed.wait(timeout=5), "User lookup blocked the event loop"
        return user

    deserialize_user.side_effect = blocking_lookup
    asyncio.get_running_loop().call_soon(progressed.set)

    assert await manager.deserialize_user_async(payload) is user
    deserialize_user.assert_called_once_with(manager, payload)


class CacheLock:
    def __init__(self):
        self.lock = threading.RLock()
        self.local = threading.local()

    @property
    def held(self):
        return getattr(self.local, "depth", 0) > 0

    def __enter__(self):
        self.lock.acquire()
        self.local.depth = getattr(self.local, "depth", 0) + 1

    def __exit__(self, *exc):
        self.local.depth -= 1
        self.lock.release()


@patch("airflow.providers.fab.auth_manager.fab_auth_manager.create_session", autospec=True)
def test_deserialize_user_synchronizes_shared_cache_without_locking_database(create_session, monkeypatch):
    managers = (FabAuthManager(), FabAuthManager())
    assert managers[0]._cache_lock is managers[1]._cache_lock
    lock = CacheLock()
    now = 0

    class CheckedCache(TTLCache):
        def __getitem__(self, key):
            assert lock.held, "Shared user cache read without its lock"
            return super().__getitem__(key)

        def __setitem__(self, key, value):
            assert lock.held, "Shared user cache write without its lock"
            return super().__setitem__(key, value)

    cache = CheckedCache(maxsize=2, ttl=1, timer=lambda: now)
    monkeypatch.setattr(FabAuthManager, "cache", cache)
    monkeypatch.setattr(FabAuthManager, "_cache_lock", lock)
    database_workers = threading.Barrier(8)

    class UserQuery:
        def __init__(self, user_id):
            self.user_id = user_id

        def one(self):
            return User(id=self.user_id, active=True)

    class UserSession:
        def scalars(self, statement):
            assert not lock.held, "Cache lock must not cover the database lookup"
            database_workers.wait(timeout=5)
            return UserQuery(statement.compile().params["id_1"])

    @contextmanager
    def session():
        yield UserSession()

    create_session.side_effect = session

    def deserialize(index):
        user_id = index % 2 + 1
        return managers[index % 2].deserialize_user({"sub": str(user_id)}).id

    with ThreadPoolExecutor(max_workers=8) as executor:
        for phase in ("cold", "warm", "expired"):
            if phase == "expired":
                now = 2
            assert list(executor.map(deserialize, range(8))) == [1, 2] * 4

    assert create_session.call_count == 16
