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

import base64
import os
import pathlib
from typing import TYPE_CHECKING, Any

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import (
    ResolvedBundle,
    convert_roots,
    extract_supervisor_schema_version,
    parse_metadata_mapping,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")

BUNDLE_FILENAME = "bundle.mjs"
METADATA_FILENAME = "airflow-metadata.yaml"
EMBEDDED_METADATA_MARKER = b"//# airflowMetadata="
EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024


def _read_embedded_metadata(bundle_path: pathlib.Path) -> dict[str, Any] | None:
    """
    Read the manifest ``airflow-ts-pack`` embeds in the bundle itself.

    The packer prepends the ``airflow-metadata.yaml`` content as a leading
    ``//# airflowMetadata=<base64>`` line comment, keeping bundle and metadata
    a single artifact. Returns ``None`` when the bundle has no such marker.
    """
    try:
        with bundle_path.open("rb") as bundle_file:
            line = bundle_file.readline(EMBEDDED_METADATA_MAX_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"cannot read {bundle_path.name}: {exc}") from exc

    if not line.startswith(EMBEDDED_METADATA_MARKER):
        return None
    if len(line) > EMBEDDED_METADATA_MAX_BYTES:
        raise ValueError(
            f"embedded airflow metadata exceeds {EMBEDDED_METADATA_MAX_BYTES} bytes; "
            f"rebuild {bundle_path.name} with airflow-ts-pack"
        )

    payload = line[len(EMBEDDED_METADATA_MARKER) :].strip()
    try:
        decoded = base64.b64decode(payload, validate=True)
    except ValueError as exc:
        raise ValueError(f"cannot parse embedded airflow metadata: {exc}") from exc
    return parse_metadata_mapping(decoded, source="embedded airflow metadata")


def _read_bundle_metadata(metadata_path: pathlib.Path) -> dict[str, Any]:
    if not metadata_path.is_file():
        raise ValueError(f"missing {METADATA_FILENAME}")

    try:
        content = metadata_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"cannot read {METADATA_FILENAME}: {exc}") from exc

    return parse_metadata_mapping(content, source=METADATA_FILENAME)


def _find_bundle(bundles_root: Sequence[pathlib.Path]) -> ResolvedBundle:
    """
    Locate the ``.mjs`` entry point in *bundles_root*.

    Scans each configured directory for ``bundle.mjs`` and reads the bundle's
    supervisor schema version from the metadata embedded in the bundle,
    falling back to a sibling ``airflow-metadata.yaml`` sidecar.

    This is an ordered fallback search, not Dag/task-aware multi-bundle
    routing. The first bundle found wins. A future version can use the
    metadata's ``dags`` section together with ``TaskInstance.dag_id`` and
    ``TaskInstance.task_id`` to select the bundle that owns a specific task.
    """
    rejected: list[tuple[pathlib.Path, str]] = []
    for root in bundles_root:
        candidate = root / BUNDLE_FILENAME
        if not candidate.is_file():
            continue
        try:
            metadata = _read_embedded_metadata(candidate)
            if metadata is None:
                metadata = _read_bundle_metadata(root / METADATA_FILENAME)
            log.debug("Selected TypeScript bundle", path=candidate, root=root)
            return ResolvedBundle(
                path=candidate,
                schema_version=extract_supervisor_schema_version(metadata),
            )
        except (TypeError, ValueError) as exc:
            log.debug(
                "TypeScript bundle metadata rejected; skipping",
                path=candidate,
                root=root,
                exc_info=True,
            )
            rejected.append((candidate.resolve(), str(exc)))

    searched = os.pathsep.join(os.fspath(p.resolve()) for p in bundles_root)
    if rejected:
        details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
        raise FileNotFoundError(
            f"Cannot find usable TypeScript bundle in {searched}: matching bundles were rejected ({details})"
        )
    raise FileNotFoundError(f"Cannot find {BUNDLE_FILENAME} in {searched}")


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
    :param bundles_root: Ordered list of directories scanned for a usable
        TypeScript bundle. Each bundle directory must contain ``bundle.mjs``
        with embedded metadata (as produced by ``airflow-ts-pack``), or
        ``bundle.mjs`` plus an ``airflow-metadata.yaml`` sidecar. This is a
        fallback search path; it does not yet route different Dag/task pairs
        to different bundles.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    node_executable: str = "node"
    bundles_root: list[pathlib.Path] = attrs.field(
        converter=convert_roots,
        validator=attrs.validators.min_len(1),
    )

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        # Multi-bundle routing should be added here by passing `what.dag_id` and
        # `what.task_id` into bundle selection and matching against metadata["dags"].
        bundle = _find_bundle(self.bundles_root)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version
