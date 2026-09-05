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
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import ResolvedBundle, convert_roots
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.coordinators.node._bundle_reader import read_bundle

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")

BUNDLE_FILENAME = "bundle.mjs"


def _select_bundle(bundles_root: Sequence[pathlib.Path], dag_id: str) -> ResolvedBundle:
    """Return the first verified configured bundle that declares *dag_id*."""
    rejected: list[tuple[pathlib.Path, str]] = []
    for root in bundles_root:
        candidate = root / BUNDLE_FILENAME
        try:
            if not candidate.is_file():
                continue
            metadata = read_bundle(candidate)
            if dag_id not in metadata.dag_ids:
                log.debug(
                    "TypeScript bundle does not contain requested Dag; skipping",
                    path=candidate,
                    root=root,
                    dag_id=dag_id,
                )
                continue
            bundle = ResolvedBundle(path=candidate, schema_version=metadata.supervisor_schema_version)
        except (OSError, TypeError, ValueError) as exc:
            log.debug(
                "TypeScript bundle rejected; skipping",
                path=candidate,
                root=root,
                reason=str(exc),
            )
            rejected.append((candidate, str(exc)))
            continue
        log.debug("Selected TypeScript bundle", path=candidate, root=root, dag_id=dag_id)
        return bundle

    searched = os.pathsep.join(os.fspath(root) for root in bundles_root)
    if rejected:
        details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
        raise FileNotFoundError(
            f"Cannot find usable TypeScript bundle containing dag_id={dag_id!r} in {searched}: "
            f"rejected candidates ({details})"
        )
    raise FileNotFoundError(f"Cannot find TypeScript bundle containing dag_id={dag_id!r} in {searched}")


@attrs.define(kw_only=True)
class NodeCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a Node.js subprocess for task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {
                    "node_executable": "node",
                    "bundles_root": ["/opt/airflow/ts-bundles"],
                },
            }
        }

    :param node_executable: Path to the ``node`` binary (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param bundles_root: Ordered list of directories scanned for the first
        verified ``bundle.mjs`` that declares the task instance's Dag.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    node_executable: str = "node"
    bundles_root: list[pathlib.Path] = attrs.field(
        converter=convert_roots,
        validator=attrs.validators.min_len(1),
    )

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        bundle = _select_bundle(self.bundles_root, what.dag_id)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version
