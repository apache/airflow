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
Helpers for system test Dags that change the state of a task or a Dag run of another Dag through the REST API.

That is what a user does in the UI. The state change is emitted by the API server instead of the worker, so the
Dags using these helpers need a deployment with an API server (the OpenLineage e2e tests), not ``dag.test()``.

The helpers run inside a task of the deployment they change, and are configured with environment variables, so
the same Dags run in the e2e tests and in other deployments:

- ``SYSTEM_TESTS_OL_API_URL``: base URL of the Airflow API server, without ``/api/...``.
- ``SYSTEM_TESTS_OL_API_TOKEN``: a token sent as ``Bearer``. When it isn't set, ``SYSTEM_TESTS_OL_API_USERNAME`` and
  ``SYSTEM_TESTS_OL_API_PASSWORD`` are used: exchanged for a token on Airflow 3, as basic auth on Airflow 2.

The REST API version follows the Airflow version the task runs on: ``/api/v2`` on Airflow 3, ``/api/v1`` on 2.
"""

from __future__ import annotations

import functools
import os
import random
import time
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

import requests

from system.openlineage.expected_events import AIRFLOW_VERSION
from system.openlineage.operator import log_validation_error

API_URL = os.getenv("SYSTEM_TESTS_OL_API_URL", "http://airflow-apiserver:8080").rstrip("/")
IS_API_V2 = AIRFLOW_VERSION.major >= 3
POLL_INTERVAL_SECONDS = 2
TOKEN_ATTEMPTS = 10
FINISHED_DAG_RUN_STATES = ("success", "failed")


def _log_errors(func: Callable) -> Callable:
    """Log whatever makes a helper fail with the validation error tag, so it shows up in the e2e test output."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            for line in filter(None, (line.strip() for line in f"{type(e).__name__}: {e}".splitlines())):
                log_validation_error("%s: %s", func.__name__, line)
            raise

    return wrapper


@functools.cache  # A token per request, polls included, overloads /auth/token
def _get_auth() -> dict[str, Any]:
    if token := os.getenv("SYSTEM_TESTS_OL_API_TOKEN"):
        return {"headers": {"Authorization": f"Bearer {token}"}}
    username = os.environ["SYSTEM_TESTS_OL_API_USERNAME"]
    password = os.environ["SYSTEM_TESTS_OL_API_PASSWORD"]
    if not IS_API_V2:
        return {"auth": (username, password)}
    for attempt in range(1, TOKEN_ATTEMPTS + 1):
        response = requests.post(
            f"{API_URL}/auth/token", json={"username": username, "password": password}, timeout=30
        )
        # The FAB auth manager of some versions fails on concurrent requests for a token, so tasks asking at the
        # same time retry at random times
        if response.status_code < 500 or attempt == TOKEN_ATTEMPTS:
            break
        time.sleep(random.uniform(0.5, 3))
    response.raise_for_status()
    return {"headers": {"Authorization": f"Bearer {response.json()['access_token']}"}}


def _request(method: str, path: str, **kwargs) -> Any:
    response = requests.request(
        method,
        f"{API_URL}/api/{'v2' if IS_API_V2 else 'v1'}/{path}",
        timeout=30,
        **_get_auth(),
        **kwargs,
    )
    if not response.ok:
        raise RuntimeError(f"{method} {path} failed with {response.status_code}: {response.text}")
    return response.json()


def _wait_for(description: str, get_result: Callable[[], Any], timeout: int) -> Any:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if result := get_result():
            return result
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Timed out after {timeout}s waiting for {description}")


def _dag_path(dag_id: str) -> str:
    return f"dags/{quote(dag_id, safe='')}"


def _dag_run_path(dag_id: str, run_id: str) -> str:
    return f"{_dag_path(dag_id)}/dagRuns/{quote(run_id, safe='')}"


def get_task_instances(dag_id: str, run_id: str) -> list[dict]:
    return _request("GET", f"{_dag_run_path(dag_id, run_id)}/taskInstances", params={"limit": 100})[
        "task_instances"
    ]


def _find_task_instance(
    task_instances: list[dict], task_id: str, map_index: int = -1, loop_iteration: int | None = None
) -> dict | None:
    return next(
        (
            ti
            for ti in task_instances
            if ti["task_id"] == task_id
            and ti["map_index"] == map_index
            and (
                loop_iteration is None or (ti.get("loop_iteration") or {}).get("iteration") == loop_iteration
            )
        ),
        None,
    )


def wait_for_task_instance(
    dag_id: str,
    run_id: str,
    task_id: str,
    *,
    states: tuple[str, ...] = ("running",),
    map_index: int = -1,
    loop_iteration: int | None = None,
    timeout: int = 180,
) -> dict:
    def get_task_instance() -> dict | None:
        ti = _find_task_instance(get_task_instances(dag_id, run_id), task_id, map_index, loop_iteration)
        return ti if ti and ti["state"] in states else None

    return _wait_for(f"task `{task_id}` of `{dag_id}` to be in {states}", get_task_instance, timeout)


def wait_for_dag_run_to_finish(dag_id: str, run_id: str, timeout: int = 180) -> str:
    def get_final_state() -> str | None:
        state = _request("GET", _dag_run_path(dag_id, run_id))["state"]
        return state if state in FINISHED_DAG_RUN_STATES else None

    return _wait_for(f"Dag run `{run_id}` of `{dag_id}` to finish", get_final_state, timeout)


def mark_task_instance(
    task_instance: dict,
    state: str,
    *,
    include_upstream: bool = False,
    include_downstream: bool = False,
) -> None:
    dag_id, run_id, task_id = task_instance["dag_id"], task_instance["dag_run_id"], task_instance["task_id"]
    include = {"include_upstream": include_upstream, "include_downstream": include_downstream}
    if not IS_API_V2 and (include_upstream or include_downstream):
        # API v1 changes the state of related tasks only through this endpoint, for a task that isn't mapped
        _request(
            "POST",
            f"{_dag_path(dag_id)}/updateTaskInstancesState",
            json={
                "dag_run_id": run_id,
                "task_id": task_id,
                "new_state": state,
                "dry_run": False,
                "include_future": False,
                "include_past": False,
                **include,
            },
        )
        return
    # API v1 only previews the change unless told otherwise, API v2 has a separate endpoint for that
    body: dict[str, Any] = {"new_state": state, **(include if IS_API_V2 else {"dry_run": False})}
    path = f"{_dag_run_path(dag_id, run_id)}/taskInstances/{quote(task_id, safe='')}"
    if task_instance.get("loop_iteration") is not None:
        # An iteration of a loop task is selected by its region
        body.update(region_id=task_instance["region_id"], region_index=task_instance["region_index"])
    elif task_instance["map_index"] != -1:
        path += f"/{task_instance['map_index']}"
    _request("PATCH", path, json=body)


def mark_task_group(dag_id: str, run_id: str, group_id: str, state: str) -> None:
    if not IS_API_V2:
        raise NotImplementedError("API v1 can't change the state of a task group")
    if (AIRFLOW_VERSION.major, AIRFLOW_VERSION.minor) < (3, 3):
        # The task group endpoint is new in Airflow 3.3, the UI marked each task of the group before
        for task_instance in get_task_instances(dag_id, run_id):
            if task_instance["task_id"].startswith(f"{group_id}."):
                mark_task_instance(task_instance, state)
        return
    _request(
        "PATCH",
        f"{_dag_run_path(dag_id, run_id)}/taskGroupInstances/{quote(group_id, safe='')}",
        json={"new_state": state},
    )


def mark_dag_run(dag_id: str, run_id: str, state: str) -> None:
    _request("PATCH", _dag_run_path(dag_id, run_id), json={"state": state})


@_log_errors
def change_state(
    dag_id: str,
    run_id: str,
    *,
    level: str,
    state: str,
    task_id: str,
    map_index: int = -1,
    wait_for_states: tuple[str, ...] = ("running",),
    target_task_id: str | None = None,
    include_upstream: bool = False,
    include_downstream: bool = False,
    wait_until_finished: bool = True,
    timeout: int = 180,
) -> None:
    """
    Wait until the task instance is in one of ``wait_for_states``, then change the state of something.

    ``level`` selects what changes:

    - ``"task"``: the task instance waited for, or ``target_task_id`` of the same Dag run, e.g. a task that hasn't
      started yet, optionally with its upstream or downstream tasks;
    - ``"task_group"``: the task group ``target_task_id``;
    - ``"dag_run"``: the Dag run.

    Returns once the Dag run has finished, so the events of the state change have been emitted, unless
    ``wait_until_finished`` is False, e.g. to change more tasks of the same Dag run.
    """
    task_instance = wait_for_task_instance(
        dag_id, run_id, task_id, states=wait_for_states, map_index=map_index, timeout=timeout
    )
    if level == "task":
        if target_task_id is not None:
            target = _find_task_instance(get_task_instances(dag_id, run_id), target_task_id)
            if target is None:
                raise ValueError(f"Task `{target_task_id}` not found in Dag run `{run_id}` of `{dag_id}`")
            task_instance = target
        mark_task_instance(
            task_instance, state, include_upstream=include_upstream, include_downstream=include_downstream
        )
    elif level == "task_group":
        if target_task_id is None:
            raise ValueError("Changing the state of a task group needs its id as `target_task_id`")
        mark_task_group(dag_id, run_id, target_task_id, state)
    else:
        mark_dag_run(dag_id, run_id, state)
    if wait_until_finished:
        wait_for_dag_run_to_finish(dag_id, run_id, timeout)


@_log_errors
def change_finished_state(dag_id: str, run_id: str, *, task_id: str, state: str, timeout: int = 180) -> None:
    """
    Wait until the Dag run finishes, then mark one of its finished tasks with another state.

    That is what a user does to correct the outcome of a task afterwards. Returns once the Dag run has finished
    again, so the events of the state change have been emitted.
    """
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)
    task_instance = _find_task_instance(get_task_instances(dag_id, run_id), task_id)
    if task_instance is None:
        raise ValueError(f"Task `{task_id}` not found in Dag run `{run_id}` of `{dag_id}`")
    mark_task_instance(task_instance, state)
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)


def _clear_task_instances(dag_id: str, run_id: str, **options) -> None:
    body: dict[str, Any] = {"dag_run_id": run_id, "dry_run": False, "only_failed": False, **options}
    _request("POST", f"{_dag_path(dag_id)}/clearTaskInstances", json=body)


@_log_errors
def clear_dag_run(dag_id: str, run_id: str, *, only_failed: bool = False, timeout: int = 180) -> None:
    """
    Wait until the Dag run finishes, then clear it, all of its tasks or only the failed ones.

    Returns once the Dag run has finished again, so the events of the rerun have been emitted.
    """
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)
    # The request queues the Dag run again before it returns, so waiting for it to finish waits for the rerun
    _request(
        "POST", f"{_dag_run_path(dag_id, run_id)}/clear", json={"dry_run": False, "only_failed": only_failed}
    )
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)


@_log_errors
def clear_tasks(
    dag_id: str,
    run_id: str,
    *,
    task_ids: list[str | tuple[str, int]] | None = None,
    task_group_id: str | None = None,
    wait_for_task_id: str | None = None,
    wait_for_states: tuple[str, ...] = ("running",),
    timeout: int = 180,
) -> None:
    """
    Clear tasks of a Dag run, like the Clear button of a task, a map index or a task group in the UI.

    By default it waits until the Dag run finishes before clearing. With ``wait_for_task_id`` it clears as soon as
    that task is in one of ``wait_for_states`` instead, e.g. to clear a task while it runs. Select the tasks with
    ``task_ids``, where a task id of a mapped task selects its whole expansion and a ``(task_id, map_index)`` pair
    one map index, or with ``task_group_id``.

    Returns once the Dag run has finished again, so the events of the rerun have been emitted.
    """
    if wait_for_task_id is None:
        wait_for_dag_run_to_finish(dag_id, run_id, timeout)
    else:
        wait_for_task_instance(dag_id, run_id, wait_for_task_id, states=wait_for_states, timeout=timeout)
    options: dict[str, Any] = {}
    if task_ids is not None:
        options["task_ids"] = task_ids
    if task_group_id is not None:
        # Listed by task id, as the `task_group_id` option only exists from Airflow 3.4
        options["task_ids"] = sorted(
            {
                ti["task_id"]
                for ti in get_task_instances(dag_id, run_id)
                if ti["task_id"].startswith(f"{task_group_id}.")
            }
        )
    # The request queues the Dag run again before it returns, so waiting for it to finish waits for the rerun
    _clear_task_instances(dag_id, run_id, **options)
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)


@_log_errors
def clear_loop_task(
    dag_id: str,
    run_id: str,
    *,
    task_id: str,
    iteration: int,
    include_later_loop_iterations: bool = False,
    timeout: int = 180,
) -> None:
    """
    Wait until the Dag run finishes, then clear the task of one loop iteration, by its execution.

    Clearing the gate of an iteration with ``include_later_loop_iterations`` runs the loop again from the next
    iteration. Returns once the Dag run has finished again, so the events of the rerun have been emitted.
    """
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)
    task_instance = _find_task_instance(get_task_instances(dag_id, run_id), task_id, loop_iteration=iteration)
    if task_instance is None:
        raise ValueError(f"Task `{task_id}` of iteration {iteration} not found in Dag run `{run_id}`")
    # The request queues the Dag run again before it returns, so waiting for it to finish waits for the rerun
    _clear_task_instances(
        dag_id,
        run_id,
        task_instance_ids=[task_instance["id"]],
        include_later_loop_iterations=include_later_loop_iterations,
    )
    wait_for_dag_run_to_finish(dag_id, run_id, timeout)


def rerun_loop(dag_id: str, run_id: str, *, gate_task_id: str, iteration: int, timeout: int = 180) -> None:
    """
    Wait until the Dag run finishes, then clear the gate of a loop iteration with the iterations after it.

    That is what a user does to run a loop again after an iteration. Returns once the Dag run has finished
    again, so the events of the rerun have been emitted.
    """
    clear_loop_task(
        dag_id,
        run_id,
        task_id=gate_task_id,
        iteration=iteration,
        include_later_loop_iterations=True,
        timeout=timeout,
    )
