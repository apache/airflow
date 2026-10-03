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
"""Node.js runtime coordinator that launches a Node.js subprocess for task execution."""

from __future__ import annotations

import os
import pathlib

import attrs

from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.coordinators.node._bundle_reader import (
    _LAYOUT_COMMENT_PREFIX,
    _read_cache_digest,
    read_bundle,
)
from airflow.sdk.execution_time.coordinator import TaskHandlerCandidate

BUNDLE_SUFFIX = ".min.mjs"


def _is_bundle(path: pathlib.Path) -> bool:
    return path.name.endswith(BUNDLE_SUFFIX)


@attrs.define(kw_only=True)
class NodeCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a Node.js subprocess for task execution.

    It also launches a verified ``*.min.mjs`` bundle to report the task handlers it registers.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {
                    "node_executable": "node",
                    "task_handler_bundle_name": "ts-task-handlers",
                },
            }
        }

    :param node_executable: Path to the ``node`` binary (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param task_handler_bundle_name: Name of the Dag bundle the Dag processor lists for
        ``*.min.mjs`` bundles. It must be registered in ``[dag_processor] dag_bundle_config_list``.
        If unset, the task's own Dag bundle is listed. A task runs the bundle its stub task was
        bound to or, for a Dag defined in TypeScript, its own Dag file, after verifying it.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    node_executable: str = "node"

    def _build_task_handler_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        metadata = read_bundle(path)
        return [self.node_executable, os.fspath(path)], metadata.supervisor_schema_version

    def _read_task_handler_candidate(
        self, path: pathlib.Path, *, rel_path: str
    ) -> TaskHandlerCandidate | None:
        if not _is_bundle(path):
            return None
        try:
            with path.open("rb") as bundle_file:
                size_bytes = os.fstat(bundle_file.fileno()).st_size
                # Any minified module may end in .min.mjs; only the layout marker makes it a bundle.
                if bundle_file.read(len(_LAYOUT_COMMENT_PREFIX)) != _LAYOUT_COMMENT_PREFIX:
                    return None
                bundle_file.seek(0)
                try:
                    cache_digest = _read_cache_digest(bundle_file, path=path)
                except ValueError as exc:
                    # The marker matched, so the file is a bundle, just not one this runtime can read.
                    return TaskHandlerCandidate(
                        rel_path=rel_path,
                        size_bytes=size_bytes,
                        cache_digest=None,
                        error=f"{rel_path}: {exc}",
                    )
        except OSError:
            return None
        return TaskHandlerCandidate(rel_path=rel_path, size_bytes=size_bytes, cache_digest=cache_digest)
