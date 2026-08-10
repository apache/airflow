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
"""
Regression guard: assert token type boundaries on Execution API routes.

``token:workload`` is a long-lived, minimal-privilege token visible in executor
queues (Celery messages, K8s pod specs).  Its security value depends on being
accepted by as few routes as possible.  See PR #62582 (foundation) and PR #66989
(two-token mechanism).

This test checks the real API router and enforces two rules:

1. Routes not listed in NON_DEFAULT_TOKEN_POLICY accept only ``token:execution``.
2. Routes listed in NON_DEFAULT_TOKEN_POLICY accept exactly the declared types.

Maintenance:
- New route with default (execution-only) scope: no test change needed.
- New route with non-default scope: add it to NON_DEFAULT_TOKEN_POLICY.
- Route removed: remove it from NON_DEFAULT_TOKEN_POLICY (if present).
- New token type: add it to the relevant entries in NON_DEFAULT_TOKEN_POLICY.
"""

from __future__ import annotations

import pytest
from fastapi.routing import APIRoute

from airflow.api_fastapi.execution_api.routes import execution_api_router

# Routes that intentionally deviate from the default (execution-only) policy.
# Any route NOT listed here must accept only {"execution"}.
NON_DEFAULT_TOKEN_POLICY: dict[str, set[str]] = {
    # The /run endpoint exchanges a workload token for a short-lived execution token.
    "PATCH /task-instances/{task_instance_id}/run": {"execution", "workload"},
    # Connection test routes run from a queued worker context (workload-only).
    "PATCH /connection-tests/{connection_test_id}": {"workload"},
    "GET /connection-tests/{connection_test_id}/connection": {"workload"},
}


def _all_route_policies() -> dict[str, set[str]]:
    """Return a map of all API routes and their allowed token types."""
    policy_map: dict[str, set[str]] = {}
    for route in execution_api_router.routes:
        if isinstance(route, APIRoute):
            allowed_tokens = set(getattr(route, "allowed_token_types", {"execution"}))
            if route.methods:
                for method in route.methods:
                    policy_map[f"{method} {route.path}"] = allowed_tokens
    return policy_map


class TestTokenScopeBoundaries:
    """Execution API routes must not silently gain or lose token type access."""

    def test_all_default_routes_are_execution_only(self):
        actual = _all_route_policies()
        non_default = {
            route: types
            for route, types in actual.items()
            if route not in NON_DEFAULT_TOKEN_POLICY and types != {"execution"}
        }

        assert not non_default, (
            "Routes gained non-default token access without being declared in "
            "NON_DEFAULT_TOKEN_POLICY:\n  "
            + "\n  ".join(f"{route}: got {sorted(tokens)}" for route, tokens in sorted(non_default.items()))
        )

    @pytest.mark.parametrize(("route", "expected"), sorted(NON_DEFAULT_TOKEN_POLICY.items()))
    def test_non_default_route_still_registered(self, route, expected):
        actual = _all_route_policies()

        assert route in actual, f"{route}: declared in policy but no longer registered"

    @pytest.mark.parametrize(("route", "expected"), sorted(NON_DEFAULT_TOKEN_POLICY.items()))
    def test_non_default_route_matches_policy(self, route, expected):
        actual = _all_route_policies()
        if route not in actual:
            pytest.skip("Route not registered (caught by test_non_default_route_still_registered)")

        tokens_gained = actual[route] - expected
        tokens_lost = expected - actual[route]

        assert not tokens_gained, f"{route}: gained unexpected token types {sorted(tokens_gained)}"
        assert not tokens_lost, f"{route}: lost expected token types {sorted(tokens_lost)}"
