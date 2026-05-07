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

import logging

log = logging.getLogger(__name__)

# Other state operations (list, get, delete per key) will be added here once the
# Core API endpoints (PR 6) land. For now, inspection is available via the REST
# API and the Task Instance detail panel in the UI.


def cleanup(args) -> None:
    """Remove expired task state rows via the configured state backend."""
    from airflow.state import get_state_backend
    from airflow.state.metastore import MetastoreStateBackend

    backend = get_state_backend()

    if args.dry_run:
        if isinstance(backend, MetastoreStateBackend):
            summary = backend._dry_run_summary()
            stale, expired = summary["stale"], summary["expired"]
            total = len(stale) + len(expired)
            if not total:
                print("Nothing to delete.")
                return
            print(f"Would delete {total} task state row(s):\n")
            if stale:
                print(f"  Older than retention period ({len(stale)}):")
                for dag_id, run_id, task_id, map_index, key in stale:
                    print(
                        f"    DAG {dag_id!r}, run {run_id!r}, task {task_id!r}, map_index {map_index!r}, key {key!r}"
                    )
            if expired:
                print(f"\n  Per-key expiry reached ({len(expired)}):")
                for dag_id, run_id, task_id, map_index, key in expired:
                    print(
                        f"    DAG {dag_id!r}, run {run_id!r}, task {task_id!r}, map_index {map_index!r}, key {key!r}"
                    )
        else:
            print("Custom backend configured — cannot preview rows.")
        return

    log.info("Running state store cleanup")
    backend.cleanup()
