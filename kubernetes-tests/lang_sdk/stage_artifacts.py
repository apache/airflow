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
Init-container entrypoint that stages the Go bundle into a pod.

This reuses the **DagBundle** machinery as the interface to the object store: it
runs the same ``get_bundle(name).initialize()`` download step that
:func:`airflow.sdk.execution_time.task_runner.parse` performs (minus the DAG
parse), so the Go binary stored in an S3 (localstack) bucket is pulled
into ``bundle.path`` = ``{dag_bundle_storage_path}/{name}``.

That path is the ``emptyDir`` shared with the task container, where it is registered
as the ``LocalDagBundle`` the Go coordinator's ``task_handler_bundle_name`` names. The
coordinator does not name the S3 bundle directly because its download drops the
binary's execute bit, which the coordinator requires, so every staged file gets the
bit back. The Java jar needs no such step and is read from its S3 bundle directly.

Configuration is read from the environment (set by the pod template):

* ``ARTIFACT_BUNDLE_NAME`` -- the bundle key in ``[dag_processor]
  dag_bundle_config_list`` to initialize.
* ``AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST`` -- registers that S3 bundle.
* ``AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_STORAGE_PATH`` -- the shared ``emptyDir``.
"""

from __future__ import annotations

import os
import stat
import sys
from pathlib import Path

import structlog

log = structlog.get_logger(logger_name=__name__)


def stage_artifacts(bundle_name: str) -> Path:
    """Initialize *bundle_name* via the DagBundle manager, make its files executable, return its path."""
    from airflow.dag_processing.bundles.manager import DagBundlesManager

    bundle = DagBundlesManager().get_bundle(name=bundle_name)
    bundle.initialize()
    root = Path(bundle.path)

    staged = sorted(p for p in root.rglob("*") if p.is_file())
    for p in staged:
        p.chmod(p.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)

    log.info(
        "Staged artifact bundle",
        bundle=bundle_name,
        path=str(root),
        files=[str(p.relative_to(root)) for p in staged],
    )
    return root


def main() -> None:
    try:
        bundle_name = os.environ["ARTIFACT_BUNDLE_NAME"]
    except KeyError:
        log.error("ARTIFACT_BUNDLE_NAME is required")
        sys.exit(2)

    root = stage_artifacts(bundle_name)

    if not any(root.rglob("*")):
        log.error("Artifact bundle is empty after staging", bundle=bundle_name, path=str(root))
        sys.exit(1)


if __name__ == "__main__":
    main()
