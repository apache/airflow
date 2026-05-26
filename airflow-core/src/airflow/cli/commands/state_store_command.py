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

from airflow.state import get_state_backend
from airflow.state.metastore import MetastoreStateBackend

log = logging.getLogger(__name__)

# Other state operations (list, get, delete per key) will be added here in the future.


def cleanup_task_states(args) -> None:
    """Remove expired task state rows (MetastoreStateBackend only)."""
    backend = get_state_backend()

    if not isinstance(backend, MetastoreStateBackend):
        print("Custom backend configured — skipping cleanup (not supported).")
        return

    if args.dry_run:
        summary = backend._summary_dry_run()
        expired = summary["expired"]
        if not expired:
            print("Nothing to delete.")
            return
        print(f"Would delete {len(expired)} task state row(s):\n")
        for dag_id, run_id, task_id, map_index, key in expired:
            print(f"  Dag {dag_id!r}, run {run_id!r}, task {task_id!r}, map_index {map_index!r}, key {key!r}")
        return

    log.info("Running task state cleanup")
    backend.cleanup()
