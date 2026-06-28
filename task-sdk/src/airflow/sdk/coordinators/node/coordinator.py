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
from typing import TYPE_CHECKING, Any

import attrs
import structlog
import yaml

from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.execution_time.schema import get_schema_version_migrator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels._generated import TaskInstance

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.node")

BUNDLE_FILENAME = "bundle.mjs"
METADATA_FILENAME = "airflow-metadata.yaml"


def _validate_schema_version(instance, _, value) -> str:
    return get_schema_version_migrator().resolve_version(str(value))


@attrs.define
class _NodeBundle:
    path: pathlib.Path
    schema_version: str = attrs.field(validator=_validate_schema_version)


def _read_bundle_metadata(metadata_path: pathlib.Path) -> dict[str, Any]:
    if not metadata_path.is_file():
        raise ValueError(f"missing {METADATA_FILENAME}")

    try:
        with metadata_path.open(encoding="utf-8") as metadata_file:
            data = yaml.safe_load(metadata_file)
    except OSError as exc:
        raise ValueError(f"cannot read {METADATA_FILENAME}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ValueError(f"cannot parse {METADATA_FILENAME}: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"{METADATA_FILENAME} must contain a mapping")
    return data


def _supervisor_schema_version(metadata: dict[str, Any]) -> str:
    sdk = metadata.get("sdk")
    if not isinstance(sdk, dict):
        raise ValueError("missing sdk metadata mapping")

    value = sdk.get("supervisor_schema_version")
    if not isinstance(value, str) or not value:
        raise ValueError("missing or invalid sdk.supervisor_schema_version")
    return value


def _find_bundle(bundles_root: Sequence[pathlib.Path]) -> _NodeBundle:
    """
    Locate the ``.mjs`` entry point in *bundles_root*.

    Scans each configured directory for ``bundle.mjs`` and reads the sibling
    ``airflow-metadata.yaml`` for the bundle's supervisor schema version.

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
            metadata = _read_bundle_metadata(root / METADATA_FILENAME)
            log.debug("Selected TypeScript bundle", path=candidate, root=root)
            return _NodeBundle(
                path=candidate,
                schema_version=_supervisor_schema_version(metadata),
            )
        except (TypeError, ValueError) as exc:
            log.debug(
                "TypeScript bundle metadata rejected; skipping", path=candidate, root=root, error=str(exc)
            )
            rejected.append((candidate.resolve(), str(exc)))

    searched = os.pathsep.join(os.fspath(p.resolve()) for p in bundles_root)
    if rejected:
        details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
        raise FileNotFoundError(
            f"Cannot find usable TypeScript bundle in {searched}: matching bundles were rejected ({details})"
        )
    raise FileNotFoundError(f"Cannot find {BUNDLE_FILENAME} in {searched}")


def _convert_bundles_root(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


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
        and ``airflow-metadata.yaml``. This is a fallback search path; it does
        not yet route different Dag/task pairs to different bundles.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    node_executable: str = "node"
    bundles_root: list[pathlib.Path] = attrs.field(
        converter=_convert_bundles_root,
        validator=attrs.validators.min_len(1),
    )

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        # Multi-bundle routing should be added here by passing `what.dag_id` and
        # `what.task_id` into bundle selection and matching against metadata["dags"].
        bundle = _find_bundle(self.bundles_root)
        return [self.node_executable, os.fspath(bundle.path)], bundle.schema_version
