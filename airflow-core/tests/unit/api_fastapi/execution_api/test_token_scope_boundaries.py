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

from typing import get_args

import pytest
from fastapi.dependencies.models import Dependant
from fastapi.dependencies.utils import get_flat_dependant
from fastapi.routing import APIRoute

from airflow.api_fastapi.execution_api.routes import execution_api_router
from airflow.api_fastapi.execution_api.security import get_selected_dag_bundle, require_dag_in_granted_bundle
from airflow.dag_processing.processor import ToManager

# Routes that intentionally deviate from the default (execution-only) policy.
# Any route NOT listed here must accept only {"execution"}.
NON_DEFAULT_TOKEN_POLICY: dict[str, set[str]] = {
    # The /run endpoint exchanges a workload token for a short-lived execution token.
    "PATCH /task-instances/{task_instance_id}/run": {"execution", "workload"},
    # Connection test routes run from a queued worker context (workload-only).
    "PATCH /connection-tests/{connection_test_id}": {"workload"},
    "GET /connection-tests/{connection_test_id}/connection": {"workload"},
    # Callback /run exchanges a single-use callback token for an execution token.
    "PATCH /callbacks/{callback_id}/run": {"callback"},
    # Requests a Dag processor makes on behalf of the code it parses and the callbacks it runs.
    "GET /connections/{connection_id:path}": {"execution", "dag_processor"},
    "GET /variables/keys": {"execution", "dag_processor"},
    "GET /variables/{variable_key:path}": {"execution", "dag_processor"},
    "PUT /variables/{variable_key:path}": {"execution", "dag_processor"},
    "DELETE /variables/{variable_key:path}": {"execution", "dag_processor"},
    "GET /task-instances/count": {"execution", "dag_processor"},
    "GET /task-instances/states": {"execution", "dag_processor"},
    "GET /task-instances/previous/{dag_id}/{task_id}": {"execution", "dag_processor"},
    "GET /dag-runs/previous": {"execution", "dag_processor"},
    "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}": {"execution", "dag_processor"},
    "HEAD /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}": {"execution", "dag_processor"},
    "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}/item/{offset}": {"execution", "dag_processor"},
    "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}/slice": {"execution", "dag_processor"},
    # The Job lifecycle of a Dag processor session.
    "POST /jobs": {"dag_processor"},
    "POST /jobs/{job_id}/heartbeat": {"dag_processor"},
    "POST /jobs/{job_id}/complete": {"dag_processor"},
}

# Routes that check the caller's Dag processor session themselves instead of requiring an open one.
SESSION_UNCHECKED_ROUTES = {"POST /jobs", "POST /jobs/{job_id}/complete"}

DAG_PROCESSOR_LIFECYCLE_ROUTES = {
    "POST /jobs",
    "POST /jobs/{job_id}/heartbeat",
    "POST /jobs/{job_id}/complete",
}

# Every message a Dag file parsing process can send its supervisor, mapped to the Execution API
# route the supervisor calls for it, or to None when it is answered without one.
DAG_PROCESSOR_MESSAGE_ROUTES: dict[str, str | None] = {
    # Published by the manager through its own endpoint, not forwarded as a runtime request.
    "DagFileParsingResult": None,
    "MaskSecret": None,
    # Bound to a task instance the processor does not have. The supervisor sends the parse process id,
    # which matches no task instance, so the answer is always empty and needs no request.
    "GetPrevSuccessfulDagRun": None,
    "GetConnection": "GET /connections/{connection_id:path}",
    "GetVariable": "GET /variables/{variable_key:path}",
    "GetVariableKeys": "GET /variables/keys",
    "PutVariable": "PUT /variables/{variable_key:path}",
    "DeleteVariable": "DELETE /variables/{variable_key:path}",
    "GetTICount": "GET /task-instances/count",
    "GetTaskStates": "GET /task-instances/states",
    "GetPreviousTI": "GET /task-instances/previous/{dag_id}/{task_id}",
    "GetPreviousDagRun": "GET /dag-runs/previous",
    "GetXCom": "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}",
    "GetXComCount": "HEAD /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}",
    "GetXComSequenceItem": "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}/item/{offset}",
    "GetXComSequenceSlice": "GET /xcoms/{dag_id}/{run_id}/{task_id}/{key:path}/slice",
}


def _get_api_routes() -> dict[str, APIRoute]:
    return {
        f"{method} {route.path}": route
        for route in execution_api_router.routes
        if isinstance(route, APIRoute)
        for method in route.methods or ()
    }


def _get_dependency_calls(dependant: Dependant) -> set:
    calls = set()
    for dependency in dependant.dependencies:
        calls.add(dependency.call)
        calls |= _get_dependency_calls(dependency)
    return calls


def _all_route_policies() -> dict[str, set[str]]:
    """Return a map of all API routes and their allowed token types."""
    return {
        key: set(getattr(route, "allowed_token_types", {"execution"}))
        for key, route in _get_api_routes().items()
    }


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


class TestDagProcessorMessageRoutes:
    """Each parse-time message is either answered locally or sent to a route that admits Dag processors."""

    def test_every_message_is_classified(self):
        (message_union, _) = get_args(ToManager)
        message_names = {message.__name__ for message in get_args(message_union)}

        assert message_names == set(DAG_PROCESSOR_MESSAGE_ROUTES)

    def test_classified_routes_are_exactly_the_dag_processor_routes(self):
        admitting = {route for route, types in _all_route_policies().items() if "dag_processor" in types}

        assert (
            admitting
            == {route for route in DAG_PROCESSOR_MESSAGE_ROUTES.values() if route}
            | DAG_PROCESSOR_LIFECYCLE_ROUTES
        )

    def test_only_job_registration_and_completion_skip_the_open_session_check(self):
        skipping = {
            key
            for key, route in _get_api_routes().items()
            if not getattr(route, "requires_open_session", True)
        }

        assert skipping == SESSION_UNCHECKED_ROUTES

    @pytest.mark.parametrize("route_key", sorted(filter(None, DAG_PROCESSOR_MESSAGE_ROUTES.values())))
    def test_dag_processor_route_is_bound_to_a_granted_bundle(self, route_key):
        route = _get_api_routes()[route_key]
        flat = get_flat_dependant(route.dependant)
        takes_dag_id = any(param.name == "dag_id" for param in (*flat.path_params, *flat.query_params))
        expected = require_dag_in_granted_bundle if takes_dag_id else get_selected_dag_bundle

        assert expected in _get_dependency_calls(route.dependant)
