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
from testcontainers.mongodb import MongoDbContainer

pytest_plugins = "tests_common.pytest_plugin"

# Pinned to a specific major to keep the image tag stable and avoid drifting
# under us when Docker Hub re-resolves :latest on every run.
MONGO_IMAGE = "mongo:8.0"


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
            yield container.get_connection_url()
        finally:
            container.stop()
        return
    raise last_exc  # pragma: no cover
