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

from fastapi.routing import APIRoute


def test_health_router_avoids_empty_root_path():
    """Regression guard for https://github.com/apache/airflow/issues/68562 (FastAPI >=0.137).

    The break is specific: a root route registered with an empty path (``@router.get("")``) *and*
    mounted under an include-time ``prefix=...`` raises ``FastAPIError: Prefix and path cannot be
    both empty`` once FastAPI switched to lazy router inclusion. Older FastAPI merged the prefix in
    eagerly and accepted it, so the version pin alone won't catch a reintroduction. The health
    router therefore declares full, non-empty paths and is included without a prefix -- assert the
    empty path stays gone, while leaving room for additional health sub-routes to be added later.
    """
    from airflow.api_fastapi.execution_api.routes import health

    empty = [route.name for route in health.router.routes if isinstance(route, APIRoute) and not route.path]
    assert not empty, f"Health routes must use explicit, non-empty paths (breaks FastAPI >=0.137): {empty}"

    paths = {route.path for route in health.router.routes if isinstance(route, APIRoute)}
    assert {"/health", "/health/ping"} <= paths
