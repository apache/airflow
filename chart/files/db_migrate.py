#
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
"""Bidirectional Airflow metadata DB reconciliation for the helm chart.

Decides at runtime whether the helm release wants a forward migrate, a
downgrade, or a no-op, and runs the right command:

* target == current  -> no-op (idempotent check)
* target  > current  -> ``airflow db migrate`` inside this job's container
  (uses the TARGET image, which ships forward scripts).
* target  < current  -> ``airflow db downgrade --to-version <target>``
  executed inside the still-running api-server pod (the OLD image still
  ships the reverse scripts), followed by scaling every DB-touching
  workload (api-server, scheduler, triggerer, dag-processor, worker) to
  zero so that no OLD pod keeps talking to the now-downgraded schema. Helm
  then patches those workloads back to ``replicas: N`` with the TARGET
  image as the upgrade proceeds, so the cluster comes back up cleanly on
  the target version. This means a downgrade trades the otherwise-broken
  rolling-update window for a brief outage (which is unavoidable when the
  schema goes backwards).

Required env:

* ``AIRFLOW_TARGET_VERSION`` - the version the chart is being upgraded/installed to.
* ``POD_NAMESPACE`` - release namespace, injected via downward API.
* ``RELEASE_NAME`` - the helm release name, used to scope the scale-down to
  only the workloads owned by this release.

Optional env:

* ``MIGRATE_JOB_DRAIN_TIMEOUT_SECONDS`` - how long to wait for DB-touching
  pods to terminate after scale-to-zero on a downgrade. Defaults to 300.
  Increase when long-running worker tasks need more time to wind down.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

from alembic.migration import MigrationContext
from kubernetes import client, config as k8s_config
from kubernetes.stream import stream
from packaging.version import Version
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    stop_after_delay,
    wait_exponential,
    wait_fixed,
)

from airflow.settings import engine

# ``_REVISION_HEADS_MAP`` is private today; a public accessor is being tracked
# upstream so the chart can drop the leading underscore in a future release.
from airflow.utils.db import _REVISION_HEADS_MAP


def _resolve_target_rev(target: str) -> str | None:
    """Return the alembic head for *target*, falling back to the nearest lower mapped version.

    ``_REVISION_HEADS_MAP`` does not list every patch release — patches often
    share the head of their minor's first release. Mirror Airflow's own CLI
    behaviour by picking the highest mapped version that is ``<= target``.
    """
    if target in _REVISION_HEADS_MAP:
        return _REVISION_HEADS_MAP[target]
    target_v = Version(target)
    candidates = [(Version(v), rev) for v, rev in _REVISION_HEADS_MAP.items() if Version(v) <= target_v]
    if not candidates:
        return None
    return max(candidates, key=lambda pair: pair[0])[1]


def _db_connect_stop(retry_state):
    # Evaluate ``DB_CONNECT_MAX_WAIT_SECONDS`` at retry time (not import time)
    # so that operators -- and unit tests -- can tune the wait window without
    # reloading the module. ``/entrypoint`` skips its DB-wait for non-airflow
    # commands (we run as ``python3 -c ...``), so on a fresh install with a
    # bundled postgres still starting, the first connect attempt races the DB.
    # Default 120s matches the entrypoint's
    # ``CONNECTION_CHECK_MAX_COUNT`` * ``CONNECTION_CHECK_SLEEP_TIME``.
    delay = int(os.environ.get("DB_CONNECT_MAX_WAIT_SECONDS", "120"))
    return stop_after_delay(delay)(retry_state)


@retry(
    stop=_db_connect_stop,
    wait=wait_fixed(3),
    retry=retry_if_exception_type(OperationalError),
    reraise=True,
)
def _wait_for_db_ready() -> None:
    """Block until the metadata DB accepts a connection.

    Called once at the top of :func:`main` so that *every* downstream step
    (``decide_action``, ``airflow db migrate`` subprocess, ``kubernetes``
    api-server pod exec) can assume the DB is reachable. Without this, the
    subprocess branch had no DB-wait of its own and would fail immediately
    on a fresh install where the bundled postgres was still initialising,
    causing ``BackoffLimitExceeded`` on the Job.
    """
    with engine.connect() as conn:
        conn.execute(text("SELECT 1"))


def decide_action(target: str) -> str:
    """Return one of ``noop``, ``forward``, ``downgrade``, ``fresh``.

    Assumes the DB is already reachable -- :func:`_wait_for_db_ready` must
    have been called first.
    """
    target_rev = _resolve_target_rev(target)
    if target_rev is None:
        # Unknown target version (e.g. dev build). Be conservative: forward only.
        return "forward"

    with engine.connect() as conn:
        current_rev = MigrationContext.configure(conn).get_current_revision()

    if current_rev is None:
        return "fresh"
    if current_rev == target_rev:
        return "noop"

    # Reverse-lookup current_rev to determine which version it belongs to,
    # then compare versions. If current_rev isn't mapped (dev/intermediate
    # alembic revision) be conservative and forward-migrate rather than risk
    # an incorrect downgrade.
    rev_to_version = {rev: ver for ver, rev in _REVISION_HEADS_MAP.items()}
    current_version = rev_to_version.get(current_rev)
    if current_version is None:
        return "forward"
    if Version(current_version) > Version(target):
        return "downgrade"
    return "forward"


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=2, min=2, max=30),
    retry=retry_if_exception_type(RuntimeError),
    reraise=True,
)
def discover_api_server_pod(namespace: str) -> str:
    """Return the name of a Running api-server pod in *namespace*.

    Retries on ``RuntimeError`` so a rolling restart of the api-server
    deployment (no Running pod for a few seconds) does not fail the job.
    """
    k8s_config.load_incluster_config()
    api = client.CoreV1Api()
    pods = api.list_namespaced_pod(
        namespace=namespace,
        label_selector="component=api-server",
        field_selector="status.phase=Running",
    ).items
    if not pods:
        raise RuntimeError(f"no Running api-server pod found in namespace {namespace}")

    # Prefer Ready pods so we don't pick one mid-rollout.
    ready = [
        p for p in pods if any(c.type == "Ready" and c.status == "True" for c in (p.status.conditions or []))
    ]
    return (ready or pods)[0].metadata.name


def run_downgrade_in_api_server(pod_name: str, namespace: str, target_version: str) -> int:
    """Exec ``airflow db downgrade`` in the api-server pod via the Kubernetes API."""
    k8s_config.load_incluster_config()
    api = client.CoreV1Api()
    command = ["airflow", "db", "downgrade", "--to-version", target_version, "--yes"]

    resp = stream(
        api.connect_get_namespaced_pod_exec,
        pod_name,
        namespace,
        container="api-server",
        command=command,
        stderr=True,
        stdin=False,
        stdout=True,
        tty=False,
        _preload_content=False,
    )
    while resp.is_open():
        resp.update(timeout=1)
        if resp.peek_stdout():
            sys.stdout.write(resp.read_stdout())
            sys.stdout.flush()
        if resp.peek_stderr():
            sys.stderr.write(resp.read_stderr())
            sys.stderr.flush()
    returncode = resp.returncode
    resp.close()
    # Treat an unknown exit code as failure rather than success: if the stream
    # closes without populating returncode we cannot prove the downgrade ran.
    if returncode is None:
        sys.stderr.write("[db_migrate] downgrade exec stream closed without an exit code\n")
        return 1
    return returncode


# Components that talk to the metadata DB and are managed by this helm release.
# A downgrade-then-scale-back sequence must drain all of them so no OLD code
# keeps talking to the now-downgraded schema before helm rolls in TARGET pods.
_DB_TOUCHING_COMPONENTS = (
    "api-server",
    "scheduler",
    "triggerer",
    "dag-processor",
    "worker",
)


def scale_release_workloads_to_zero(
    namespace: str, release_name: str, timeout_seconds: int | None = None
) -> None:
    """Scale all DB-touching workloads of this release to 0 and wait for drain.

    Helm will patch the same Deployments/StatefulSets back to ``replicas: N``
    when it applies the post-hook manifests, so we deliberately do NOT scale
    them up again here.

    The drain deadline defaults to ``MIGRATE_JOB_DRAIN_TIMEOUT_SECONDS`` (or
    300s if unset). Operators with long-running worker tasks can raise this
    via the ``migrateDatabaseJob.drainTimeoutSeconds`` chart value.
    """
    if timeout_seconds is None:
        timeout_seconds = int(os.environ.get("MIGRATE_JOB_DRAIN_TIMEOUT_SECONDS", "300"))
    k8s_config.load_incluster_config()
    apps = client.AppsV1Api()
    core = client.CoreV1Api()

    selector = f"release={release_name},component in ({','.join(_DB_TOUCHING_COMPONENTS)})"
    scale_body = {"spec": {"replicas": 0}}

    deployments = apps.list_namespaced_deployment(namespace, label_selector=selector).items
    statefulsets = apps.list_namespaced_stateful_set(namespace, label_selector=selector).items

    for d in deployments:
        print(f"[db_migrate] scaling Deployment/{d.metadata.name} to 0", flush=True)
        apps.patch_namespaced_deployment_scale(d.metadata.name, namespace, scale_body)
    for s in statefulsets:
        print(f"[db_migrate] scaling StatefulSet/{s.metadata.name} to 0", flush=True)
        apps.patch_namespaced_stateful_set_scale(s.metadata.name, namespace, scale_body)

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        remaining = core.list_namespaced_pod(namespace, label_selector=selector).items
        if not remaining:
            print("[db_migrate] all DB-touching pods drained.", flush=True)
            return
        print(
            f"[db_migrate] waiting for {len(remaining)} pod(s) to terminate...",
            flush=True,
        )
        time.sleep(2)
    raise TimeoutError(f"DB-touching pods did not drain within {timeout_seconds}s after scale-to-zero")


def main() -> int:
    target = os.environ.get("AIRFLOW_TARGET_VERSION")
    namespace = os.environ.get("POD_NAMESPACE")
    release_name = os.environ.get("RELEASE_NAME")
    if not target:
        raise SystemExit("AIRFLOW_TARGET_VERSION must be set")
    if not namespace:
        raise SystemExit("POD_NAMESPACE must be set")

    # Block until the DB is reachable so that BOTH ``decide_action`` and the
    # subsequent ``airflow db migrate`` subprocess (which does not go through
    # ``/entrypoint`` and therefore has no DB-wait of its own) succeed on a
    # fresh install where the bundled postgres may still be initialising.
    try:
        _wait_for_db_ready()
    except OperationalError as exc:
        print(f"[db_migrate] DB unreachable after wait window: {exc}", flush=True)
        return 1

    action = decide_action(target)
    print(f"[db_migrate] target={target} action={action}", flush=True)

    if action == "noop":
        print("[db_migrate] DB already at target revision, nothing to do.")
        return 0
    if action in {"fresh", "forward"}:
        return subprocess.run(["airflow", "db", "migrate"], check=False).returncode
    if action == "downgrade":
        if not release_name:
            raise SystemExit("RELEASE_NAME must be set for the downgrade branch")
        pod = discover_api_server_pod(namespace)
        print(f"[db_migrate] downgrading via api-server pod {pod}", flush=True)
        rc = run_downgrade_in_api_server(pod, namespace, target)
        if rc != 0:
            return rc
        # Drain OLD pods before helm rolls in TARGET pods to avoid OLD code
        # running against the now-downgraded schema (see module docstring).
        scale_release_workloads_to_zero(namespace, release_name)
        return 0
    raise SystemExit(f"unknown action: {action}")


if __name__ == "__main__":
    sys.exit(main())
