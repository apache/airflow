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
  ships the reverse scripts).

Required env:

* ``AIRFLOW_TARGET_VERSION`` - the version the chart is being upgraded/installed to.
* ``POD_NAMESPACE`` - release namespace, injected via downward API.

Reference: https://github.com/apache/airflow/issues/68072
"""

from __future__ import annotations

import os
import subprocess
import sys


def decide_action(target: str) -> str:
    """Return one of ``noop``, ``forward``, ``downgrade``, ``fresh``."""
    # NOTE: _REVISION_HEADS_MAP is a private symbol in airflow.utils.db.
    # Tracked in #68072 to expose a public accessor; using the private name is
    # the only way today to map a target version string to an alembic revision.
    from airflow.utils.db import _REVISION_HEADS_MAP

    target_rev = _REVISION_HEADS_MAP.get(target)
    if target_rev is None:
        # Unknown target version (e.g. dev build). Be conservative: forward only.
        return "forward"

    try:
        from alembic.migration import MigrationContext

        from airflow.settings import engine

        with engine.connect() as conn:
            current_rev = MigrationContext.configure(conn).get_current_revision()
    except Exception:
        # DB unreachable or alembic_version table missing -> treat as fresh install.
        return "fresh"

    if current_rev is None:
        return "fresh"
    if current_rev == target_rev:
        return "noop"

    # Walk the alembic revision graph to decide direction. If target is an
    # ancestor of current we need to downgrade; otherwise forward migrate.
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    import airflow

    cfg = Config()
    cfg.set_main_option(
        "script_location",
        os.path.join(os.path.dirname(airflow.__file__), "migrations"),
    )
    script = ScriptDirectory.from_config(cfg)
    ancestors_of_current = {rev.revision for rev in script.walk_revisions("base", current_rev)}
    if target_rev in ancestors_of_current and target_rev != current_rev:
        return "downgrade"
    return "forward"


def discover_api_server_pod(namespace: str) -> str:
    """Return the name of a Running api-server pod in *namespace*."""
    from kubernetes import client, config

    config.load_incluster_config()
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
    from kubernetes import client, config
    from kubernetes.stream import stream

    config.load_incluster_config()
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
    return returncode or 0


def main() -> int:
    target = os.environ.get("AIRFLOW_TARGET_VERSION")
    namespace = os.environ.get("POD_NAMESPACE")
    if not target:
        raise SystemExit("AIRFLOW_TARGET_VERSION must be set")
    if not namespace:
        raise SystemExit("POD_NAMESPACE must be set")

    action = decide_action(target)
    print(f"[db_migrate] target={target} action={action}", flush=True)

    if action == "noop":
        print("[db_migrate] DB already at target revision, nothing to do.")
        return 0
    if action in {"fresh", "forward"}:
        return subprocess.call(["airflow", "db", "migrate"])
    if action == "downgrade":
        pod = discover_api_server_pod(namespace)
        print(f"[db_migrate] downgrading via api-server pod {pod}", flush=True)
        return run_downgrade_in_api_server(pod, namespace, target)
    raise SystemExit(f"unknown action: {action}")


if __name__ == "__main__":
    sys.exit(main())
