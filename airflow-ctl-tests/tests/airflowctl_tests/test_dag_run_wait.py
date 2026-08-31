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

from itertools import islice

import pytest
import requests

from airflowctl_tests import conftest


class _JsonResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def http_error(status_code: int) -> requests.exceptions.HTTPError:
    response = requests.Response()
    response.status_code = status_code
    return requests.exceptions.HTTPError(str(status_code), response=response)


def test_poll_delays_grow_and_cap():
    assert list(islice(conftest._compute_poll_delays(), 8)) == [0.5, 1.0, 2.0, 4.0, 8.0, 10.0, 10.0, 10.0]


@pytest.mark.parametrize("terminal_state", ["success", "failed"])
def test_wait_returns_as_soon_as_the_dag_run_is_terminal(monkeypatch, terminal_state):
    states = iter(["queued", "running", terminal_state])
    slept: list[float] = []
    monkeypatch.setattr(conftest, "_find_dag_run_state", lambda dag_id, dag_run_id: next(states))
    monkeypatch.setattr(conftest.time, "sleep", slept.append)

    conftest._wait_for_dag_run_terminal_state("example_bash_operator", "manual__1", timeout=300)

    assert slept == [0.5, 1.0]


def test_wait_spends_its_whole_budget_and_no_more(monkeypatch):
    now = [1000.0]
    slept: list[float] = []

    def sleep(seconds):
        slept.append(seconds)
        now[0] += seconds

    monkeypatch.setattr(conftest, "_find_dag_run_state", lambda dag_id, dag_run_id: "running")
    monkeypatch.setattr(conftest, "_describe_task_instances", lambda dag_id, dag_run_id: "runme_0=queued")
    monkeypatch.setattr(conftest.time, "monotonic", lambda: now[0])
    monkeypatch.setattr(conftest.time, "sleep", sleep)

    with pytest.raises(TimeoutError):
        conftest._wait_for_dag_run_terminal_state("example_bash_operator", "manual__1", timeout=5)

    # The last delay is clamped so the wait stops at the deadline instead of overshooting it.
    assert slept == [0.5, 1.0, 2.0, 1.5]


def test_wait_timeout_reports_dag_run_and_task_instance_states(monkeypatch):
    monkeypatch.setattr(conftest, "_find_dag_run_state", lambda dag_id, dag_run_id: "running")
    monkeypatch.setattr(
        conftest, "_describe_task_instances", lambda dag_id, dag_run_id: "runme_0=queued, runme_1=None"
    )

    with pytest.raises(TimeoutError) as exc_info:
        conftest._wait_for_dag_run_terminal_state("example_bash_operator", "manual__1", timeout=0)

    message = str(exc_info.value)
    assert "example_bash_operator/manual__1" in message
    assert "Dag run state: running" in message
    assert "runme_0=queued, runme_1=None" in message
    assert conftest.DAG_RUN_WAIT_TIMEOUT_ENV in message


@pytest.mark.parametrize(
    ("error", "retries"),
    [
        (requests.exceptions.ConnectionError("connection refused"), True),
        (http_error(503), True),
        (http_error(404), False),
        (http_error(401), False),
    ],
    ids=["connection-error", "server-error", "missing-dag-run", "rejected-token"],
)
def test_find_dag_run_state_retries_only_transient_failures(monkeypatch, error, retries):
    def raise_error(path):
        raise error

    monkeypatch.setattr(conftest, "_request_api", raise_error)

    if retries:
        assert conftest._find_dag_run_state("example_bash_operator", "manual__1") is None
    else:
        with pytest.raises(requests.exceptions.HTTPError):
            conftest._find_dag_run_state("example_bash_operator", "manual__1")


def test_access_token_is_obtained_once_per_session(monkeypatch):
    logins: list[tuple[str, str, str]] = []
    monkeypatch.setattr(conftest._CtlTestState, "access_token", None)
    monkeypatch.setattr(
        conftest,
        "generate_access_token",
        lambda username, password, host: logins.append((username, password, host)) or "token",
    )

    assert conftest._get_access_token() == "token"
    assert conftest._get_access_token() == "token"
    assert logins == [(conftest.API_USERNAME, conftest.API_PASSWORD, conftest.DOCKER_COMPOSE_HOST_PORT)]


def test_request_api_targets_the_compose_stack_with_the_session_token(monkeypatch):
    calls: list[tuple[str, dict]] = []
    monkeypatch.setattr(conftest, "_get_access_token", lambda: "token")
    monkeypatch.setattr(
        conftest.requests,
        "get",
        lambda url, **kwargs: calls.append((url, kwargs)) or _JsonResponse({"state": "success"}),
    )

    assert conftest._request_api("dags/example_bash_operator/dagRuns/manual__1") == {"state": "success"}

    url, kwargs = calls[0]
    assert url == (
        f"http://{conftest.DOCKER_COMPOSE_HOST_PORT}/api/v2/dags/example_bash_operator/dagRuns/manual__1"
    )
    assert kwargs["headers"] == {"Authorization": "Bearer token"}
    assert kwargs["timeout"] == conftest._API_REQUEST_TIMEOUT


@pytest.mark.parametrize(
    ("task_instances", "expected"),
    [
        (
            [{"task_id": "runme_0", "state": "queued"}, {"task_id": "run_this_last", "state": None}],
            "runme_0=queued, run_this_last=None",
        ),
        ([], "none"),
    ],
    ids=["renders-each-task", "no-task-instances"],
)
def test_describe_task_instances_renders_every_task_state(monkeypatch, task_instances, expected):
    monkeypatch.setattr(conftest, "_request_api", lambda path: {"task_instances": task_instances})

    assert conftest._describe_task_instances("example_bash_operator", "manual__1") == expected


def test_describe_task_instances_reports_an_unreachable_api(monkeypatch):
    def raise_connection_error(path):
        raise requests.exceptions.ConnectionError("connection refused")

    monkeypatch.setattr(conftest, "_request_api", raise_connection_error)

    assert conftest._describe_task_instances("example_bash_operator", "manual__1") == (
        "unavailable (connection refused)"
    )


def test_waiter_spends_the_budget_once_per_dag_run(monkeypatch):
    waited: list[tuple[str, str]] = []
    monkeypatch.setattr(
        conftest,
        "_wait_for_dag_run_terminal_state",
        lambda dag_id, dag_run_id: waited.append((dag_id, dag_run_id)),
    )
    wait = conftest._build_dag_run_waiter()

    wait("example_bash_operator", "manual__1")
    wait("example_bash_operator", "manual__1")
    wait("example_bash_operator", "manual__2")

    assert waited == [("example_bash_operator", "manual__1"), ("example_bash_operator", "manual__2")]


def _build_timed_out_waiter(monkeypatch):
    def raise_timeout(dag_id, dag_run_id):
        raise TimeoutError("did not reach a terminal state")

    monkeypatch.setattr(conftest, "_wait_for_dag_run_terminal_state", raise_timeout)
    wait = conftest._build_dag_run_waiter()
    with pytest.raises(TimeoutError):
        wait("example_bash_operator", "manual__1")
    return wait


def test_waiter_skips_later_commands_instead_of_timing_out_again(monkeypatch):
    wait = _build_timed_out_waiter(monkeypatch)
    monkeypatch.setattr(conftest, "_find_dag_run_state", lambda dag_id, dag_run_id: "running")

    with pytest.raises(pytest.skip.Exception, match="did not reach a terminal state"):
        wait("example_bash_operator", "manual__1")


def test_waiter_lets_later_commands_through_when_the_dag_run_finished_late(monkeypatch):
    wait = _build_timed_out_waiter(monkeypatch)
    monkeypatch.setattr(conftest, "_find_dag_run_state", lambda dag_id, dag_run_id: "success")

    try:
        wait("example_bash_operator", "manual__1")
    except pytest.skip.Exception:
        pytest.fail("a Dag run that finished late must not skip the remaining xcom commands")
