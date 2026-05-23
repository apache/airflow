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

import contextlib
import time

import pytest
from pymongo import MongoClient
from testcontainers.mongodb import MongoDbContainer

pytest_plugins = "tests_common.pytest_plugin"

# Pinned to a specific major to keep the image tag stable and avoid drifting
# under us when Docker Hub re-resolves :latest on every run.
MONGO_IMAGE = "mongo:8.0"


def _wait_for_mongo_ready(url: str, timeout: int = 60) -> None:
    """Poll mongod via ``ping`` until it answers or the timeout expires.

    ``MongoDbContainer.start()`` only waits for the ``waiting for connections``
    log line, which mongod can emit a few milliseconds before the TCP listener
    is actually accepting on the published port. Under
    ``--resolution lowest-direct`` (testcontainers 4.12.0, pymongo 4.13.2) we
    occasionally observed every ``TestMongoHook`` test failing with
    ``Connection refused`` on the very first ``MongoHook.get_conn()`` call.
    Actively pinging mongod after the log gate closes the gap.
    """
    deadline = time.monotonic() + timeout
    last_exc: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with MongoClient(url, serverSelectionTimeoutMS=2000) as client:
                client.admin.command("ping")
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(1)
    raise TimeoutError(f"mongod at {url} did not answer ping within {timeout}s") from last_exc


@pytest.fixture(scope="session")
def mongodb_container():
    # Retry container start to absorb transient Docker Hub failures (e.g.
    # registry-auth timeouts) so a single registry blip does not cascade
    # into setup errors across the whole mongo test suite.
    last_exc: Exception | None = None
    for attempt in range(3):
        container = MongoDbContainer(MONGO_IMAGE)
        try:
            container.start()
        except Exception as exc:
            last_exc = exc
            with contextlib.suppress(Exception):
                container.stop()
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
                continue
            raise
        try:
            url = container.get_connection_url()
            _wait_for_mongo_ready(url)
            yield url
        finally:
            container.stop()
        return
    raise last_exc  # pragma: no cover
