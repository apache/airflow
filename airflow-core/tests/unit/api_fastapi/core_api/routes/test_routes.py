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

from airflow.api_fastapi.core_api.routes.public import authenticated_router, public_router

# Set of paths that are allowed to be accessible without authentication
NO_AUTH_PATHS = {
    "/api/v2/auth/login",
    "/api/v2/auth/logout",
    "/api/v2/auth/refresh",
    "/api/v2/version",
    "/api/v2/monitor/health",
}


def test_no_auth_routes():
    """
    Verify that only the routes with NO_AUTH_PATHS are excluded from the `authenticated_router`. This
    test ensures that a router is not added to the non-authenticated router by mistake.
    """
    paths_in_public_router = {route.path for route in public_router.routes}
    paths_in_authenticated_router = {
        f"{public_router.prefix}{route.path}" for route in authenticated_router.routes
    }
    assert paths_in_public_router - paths_in_authenticated_router == NO_AUTH_PATHS


def test_routes_with_responses():
    """Verify that each route in `public_router` has appropriate responses configured."""
    for route in public_router.routes:
        if route.path in NO_AUTH_PATHS:
            # Routes in NO_AUTH_PATHS should not have 401 or 403 response codes
            assert 401 not in route.responses, f"Route {route.path} should not require auth (401)"
            assert 403 not in route.responses, f"Route {route.path} should not require auth (403)"
        else:
            # All other routes should have 401 and 403 responses indicating they require auth
            assert 401 in route.responses, f"Route {route.path} is missing 401 response"
            assert 403 in route.responses, f"Route {route.path} is missing 403 response"


def test_invalid_routes_return_404(test_client):
    """Invalid routes should return a 404."""
    response = test_client.get("/api/v2/nonexistent")
    assert response.status_code == 404
    assert response.json() == {"error": "API route not found"}

    response = test_client.get("/api/nonexistent")
    assert response.status_code == 404
    assert response.json() == {"error": "API route not found"}
