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
"""DB-free, xdist-safe execution of a task through a *real* supervisor socket.

`run_task` (in ``pytest_plugin``) mocks supervisor comms entirely in-process and
has **no real socket**, so operators that spawn a subprocess which re-connects to
the supervisor — ``PythonVirtualenvOperator``, ``ExternalPythonOperator``,
``run_as_user`` — fail there with ``OSError: Socket operation on non-socket``.

This drives the *real* ``InProcessTestSupervisor`` (its socketpair machinery is
created specifically for VirtualEnv operators) but injects a **dry-run Execution-API
client** instead of the DB-backed in-process API server, so the subprocess gets a
working supervisor socket without touching the metadata DB. Tests using it need no
``@pytest.mark.db_test`` and run under xdist.

The client is the real ``Client(dry_run=True)`` (which already fakes the run
context and no-ops heartbeats via ``noop_handler``), with the discarding transport
swapped for one that *remembers* XCom writes in an in-memory dict — exposed as
``client.pushed_xcoms`` so tests can assert what a task pushed.

Requires the Task SDK ``run_task_in_process(..., client=)`` parameter (newer than
Airflow 3.0/3.1, and absent on 2.x); callers must gate on its availability and fall
back to a DB-backed path otherwise.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.supervisor import TaskRunResult
    from airflow.sdk.types import Operator

# XCom is the only resource that must round-trip; the run-context is fed back from the
# (valid) ti_context the test built, and everything else (heartbeat, state updates) is the
# stock ``noop_handler``. (``noop_handler``'s own run-context is stale vs the live schema.)
_XCOM_PATH_PARTS = 5  # /xcoms/{dag_id}/{run_id}/{task_id}/{key}


def _remembering_handler(store: dict, run_context_json: bytes) -> Callable:
    """A dry-run transport handler: valid run-context + XCom round-trip from ``store``, else no-op."""
    import httpx

    from airflow.sdk.api.client import noop_handler

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/task-instances/") and path.endswith("/run"):
            return httpx.Response(200, content=run_context_json)
        parts = path.strip("/").split("/")
        if len(parts) == _XCOM_PATH_PARTS and parts[0] == "xcoms":
            dag_id, run_id, task_id, key = parts[1:]
            sig = (dag_id, run_id, task_id, key)
            if request.method == "POST":
                store[sig] = json.loads(request.content)
                return httpx.Response(201, json={"ok": True})
            if request.method == "GET":
                if sig in store:
                    return httpx.Response(200, json={"key": key, "value": store[sig]})
                return httpx.Response(404, json={"detail": "XCom not found"})
        return noop_handler(request)

    return handler


def build_in_memory_client(ti_context) -> Client:
    """A real ``Client(dry_run=True)`` that remembers XCom writes (no DB, no network).

    ``ti_context`` (a ``TIRunContext``) is replayed for the task-start request. Pushed XCom
    values are exposed as ``client.pushed_xcoms`` keyed by ``(dag_id, run_id, task_id, key)``.
    """
    import httpx

    from airflow.sdk.api.client import Client

    store: dict[tuple[str, str, str, str], Any] = {}
    client = Client(
        base_url=None,
        dry_run=True,
        token="",
        transport=httpx.MockTransport(_remembering_handler(store, ti_context.model_dump_json().encode())),
    )
    client.pushed_xcoms = store  # type: ignore[attr-defined]
    return client


def pushed_xcom(xcoms: dict, ti, key: str = "return_value") -> Any:
    """Read an XCom a task pushed during :func:`run_task_no_db` (``None`` if absent)."""
    return xcoms.get((ti.dag_id, ti.run_id, ti.task_id, key))


def run_task_no_db(
    task: Operator,
    create_runtime_ti: Callable[..., Any],
    *,
    logical_date: Any | None = None,
) -> tuple[TaskRunResult, dict]:
    """Run *task* DB-free through the real-socket in-process supervisor.

    Returns ``(result, pushed_xcoms)`` where ``result`` is the stock ``TaskRunResult``
    (``.state`` / ``.msg`` / ``.error`` / ``.ti``) and ``pushed_xcoms`` is the dict of
    XCom values the task pushed (read via :func:`pushed_xcom`).
    """
    from uuid6 import uuid7

    from airflow.sdk.api.datamodels._generated import TaskInstance as TaskInstanceDTO
    from airflow.sdk.execution_time.supervisor import run_task_in_process

    ti_kwargs = {} if logical_date is None else {"logical_date": logical_date}
    rti = create_runtime_ti(task, **ti_kwargs)

    # `start()` model_dumps `what`; the plain DTO dumps cleanly, whereas the
    # operator-laden RuntimeTaskInstance trips forward refs (RetryPolicy/WeightRuleParam).
    what = TaskInstanceDTO(
        id=rti.id,
        task_id=rti.task_id,
        dag_id=rti.dag_id,
        run_id=rti.run_id,
        try_number=rti.try_number,
        map_index=rti.map_index,
        dag_version_id=uuid7(),
        queue="default",
    )

    client = build_in_memory_client(rti._ti_context_from_server)
    result = run_task_in_process(what, task, client=client)
    return result, client.pushed_xcoms  # type: ignore[attr-defined]
