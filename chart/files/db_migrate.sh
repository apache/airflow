#!/usr/bin/env bash
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

# Bidirectional Airflow metadata DB reconciliation script.
#
# Decides at runtime whether the helm release wants a forward migrate,
# a downgrade, or a no-op, and runs the right command:
#
#   - target == current  -> no-op (idempotent check)
#   - target  > current  -> "airflow db migrate" inside this job's container
#                            (uses the TARGET image, which ships forward scripts)
#   - target  < current  -> "airflow db downgrade --to-version <target>"
#                            executed inside the still-running api-server pod
#                            (the OLD image still ships the reverse scripts)
#
# Required env:
#   AIRFLOW_TARGET_VERSION  - the version the chart is being upgraded/installed to
#   POD_NAMESPACE           - release namespace, injected via downward API
#
# Reference: https://github.com/apache/airflow/issues/68072

set -euo pipefail

: "${AIRFLOW_TARGET_VERSION:?AIRFLOW_TARGET_VERSION must be set}"
: "${POD_NAMESPACE:?POD_NAMESPACE must be set}"

# Compare the target version against the alembic head currently in the DB.
# Prints one of: "noop", "forward", "downgrade", "fresh".
#
# We do this via a small Python helper rather than parsing CLI output because
# alembic revisions are SHA-like identifiers and only the in-process Airflow
# code knows the revision graph.
decide_action() {
  python3 - <<'PY'
import os
import sys

target = os.environ["AIRFLOW_TARGET_VERSION"]

# NOTE: _REVISION_HEADS_MAP is currently a private symbol in airflow.utils.db.
# Tracked in #68072 to expose a public accessor; using the private name is the
# only way today to map a target version string to an alembic revision.
from airflow.utils.db import _REVISION_HEADS_MAP  # noqa: E402

target_rev = _REVISION_HEADS_MAP.get(target)
if target_rev is None:
    # Unknown target version (e.g. dev build). Be conservative: forward only.
    print("forward")
    sys.exit(0)

try:
    from alembic.migration import MigrationContext
    from airflow.settings import engine

    with engine.connect() as conn:
        current_rev = MigrationContext.configure(conn).get_current_revision()
except Exception:
    # DB unreachable or alembic_version table missing -> treat as fresh install.
    print("fresh")
    sys.exit(0)

if current_rev is None:
    print("fresh")
    sys.exit(0)

if current_rev == target_rev:
    print("noop")
    sys.exit(0)

# Walk the alembic revision graph to decide direction. If target is an ancestor
# of current we need to downgrade; otherwise forward migrate.
from alembic.config import Config
from alembic.script import ScriptDirectory
import airflow

script_location = os.path.join(os.path.dirname(airflow.__file__), "migrations")
cfg = Config()
cfg.set_main_option("script_location", script_location)
script = ScriptDirectory.from_config(cfg)

ancestors_of_current = {rev.revision for rev in script.walk_revisions("base", current_rev)}
if target_rev in ancestors_of_current and target_rev != current_rev:
    print("downgrade")
else:
    print("forward")
PY
}

# Discover the api-server pod that is still running the OLD image. Returns the
# pod name on stdout, exit code 0 if found.
discover_api_server_pod() {
  python3 - <<'PY'
import os
import sys

from kubernetes import client, config

config.load_incluster_config()
api = client.CoreV1Api()
namespace = os.environ["POD_NAMESPACE"]

pods = api.list_namespaced_pod(
    namespace=namespace,
    label_selector="component=api-server",
    field_selector="status.phase=Running",
).items

if not pods:
    sys.stderr.write("no Running api-server pod found in namespace %s\n" % namespace)
    sys.exit(1)

# Prefer Ready pods so we don't pick one mid-rollout.
ready = [
    p for p in pods
    if any(c.type == "Ready" and c.status == "True" for c in (p.status.conditions or []))
]
target = (ready or pods)[0]
print(target.metadata.name)
PY
}

# Run `airflow db downgrade` inside the api-server pod using a Python exec
# against the Kubernetes API (no kubectl binary required in this image).
run_downgrade_in_api_server() {
  local pod_name="$1"
  local target_version="$2"

  python3 - "$pod_name" "$target_version" <<'PY'
import os
import sys

from kubernetes import client, config
from kubernetes.stream import stream

pod_name = sys.argv[1]
target_version = sys.argv[2]
namespace = os.environ["POD_NAMESPACE"]

config.load_incluster_config()
api = client.CoreV1Api()

command = [
    "airflow", "db", "downgrade",
    "--to-version", target_version,
    "--yes",
]

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

exit_code = 0
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
sys.exit(returncode or 0)
PY
}

action="$(decide_action)"
echo "[db_migrate.sh] target=${AIRFLOW_TARGET_VERSION} action=${action}"

case "${action}" in
  noop)
    echo "[db_migrate.sh] DB already at target revision, nothing to do."
    ;;
  fresh|forward)
    exec airflow db migrate
    ;;
  downgrade)
    pod="$(discover_api_server_pod)"
    echo "[db_migrate.sh] downgrading via api-server pod ${pod}"
    run_downgrade_in_api_server "${pod}" "${AIRFLOW_TARGET_VERSION}"
    ;;
  *)
    echo "[db_migrate.sh] unknown action: ${action}" >&2
    exit 1
    ;;
esac
