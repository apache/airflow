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
"""Node.js runtime coordinator that launches a Node.js subprocess for task execution and Dag parsing."""

from __future__ import annotations

import os
import pathlib
from typing import TYPE_CHECKING

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import (
    ResolvedBundle,
    walk_files,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.coordinators.node._bundle_reader import BUNDLE_SUFFIX, read_bundle
from airflow.sdk.coordinators.node._dag_importer import NodeDagImporter

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")


def _is_bundle(path: pathlib.Path) -> bool:
    return path.name.endswith(BUNDLE_SUFFIX)


@attrs.define
class _Bundle(ResolvedBundle):
    @classmethod
    def find(cls, roots: Sequence[pathlib.Path], dag_id: str) -> Self:
        """Return the first verified configured bundle that declares *dag_id*."""
        log.debug("Finding TypeScript bundles recursively", roots=roots, dag_id=dag_id)
        rejected: list[tuple[pathlib.Path, str]] = []
        for candidate in walk_files(roots, match=_is_bundle):
            try:
                metadata = read_bundle(candidate)
                if dag_id not in metadata.dag_ids:
                    log.debug(
                        "TypeScript bundle does not contain requested Dag; skipping",
                        path=candidate,
                        dag_id=dag_id,
                    )
                    rejected.append(
                        (candidate, f"verified bundle declares dag_ids={sorted(metadata.dag_ids)!r}")
                    )
                    continue
                bundle = cls(path=candidate, schema_version=metadata.supervisor_schema_version)
            except (OSError, TypeError, ValueError) as exc:
                log.debug(
                    "TypeScript bundle rejected; skipping",
                    path=candidate,
                    reason=str(exc),
                    exc_info=True,
                )
                rejected.append((candidate, str(exc)))
                continue
            log.debug("Selected TypeScript bundle", path=candidate, dag_id=dag_id)
            return bundle

        searched = os.pathsep.join(os.fspath(root) for root in roots)
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
    Coordinator that launches a Node.js subprocess for task execution and Dag parsing.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "ts": {
                "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
                "kwargs": {"node_executable": "node"},
            }
        }

    :param node_executable: Path to the ``node`` binary (defaults to
        ``"node"``, which relies on ``$PATH``).
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.

    A task of a native TypeScript Dag runs the ``*.min.mjs`` bundle its Dag was parsed from.
    Otherwise, the Dag bundle is searched recursively for the first verified ``*.min.mjs`` bundle
    that declares the task instance's Dag. The coordinator also parses the native Dags of the
    ``*.min.mjs`` bundles in the Dag bundles it serves.
    """

    node_executable: str = "node"

    def _build_execute_task_command(
        self, *, what: TaskInstance, dag_file: pathlib.Path | None = None
    ) -> tuple[list[str], str | None]:
        roots = self._get_scan_roots()
        if (bundle := self._find_dag_bundle(roots, dag_file, what.dag_id)) is None:
            bundle = _Bundle.find(roots, what.dag_id)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version

    @staticmethod
    def _find_dag_bundle(
        roots: Sequence[pathlib.Path], dag_file: pathlib.Path | None, dag_id: str
    ) -> _Bundle | None:
        """Return *dag_file* when it is a bundle under *roots* that declares *dag_id*, or ``None``."""
        if dag_file is None or not _is_bundle(dag_file):
            return None
        resolved = dag_file.resolve()
        if not any(resolved.is_relative_to(root.resolve()) for root in roots):
            return None
        try:
            metadata = read_bundle(dag_file)
        except (OSError, TypeError, ValueError) as exc:
            log.debug("Cannot run the Dag's own TypeScript bundle", path=dag_file, reason=str(exc))
            return None
        if dag_id not in metadata.dag_ids:
            return None
        return _Bundle(path=dag_file, schema_version=metadata.supervisor_schema_version)

    def get_dag_importer(self) -> NodeDagImporter:
        return NodeDagImporter(coordinator=self)

    def _build_parse_dag_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        return [self.node_executable, os.fspath(path)], read_bundle(path).supervisor_schema_version
