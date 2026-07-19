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

from __future__ import annotations

import base64
import pathlib

import pytest
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.coordinators.node.coordinator import (
    EMBEDDED_METADATA_MAX_BYTES,
    NodeCoordinator,
    _find_bundle,
)

SCHEMA_VERSION = "2026-06-16"


def _make_ti(dag_id: str = "test_dag", queue: str = "ts") -> TaskInstance:
    return TaskInstance(
        id=uuid7(),
        dag_version_id=uuid7(),
        task_id="test_task",
        dag_id=dag_id,
        run_id="run_1",
        try_number=1,
        map_index=-1,
        queue=queue,
    )


def _metadata_yaml(schema_version: str) -> str:
    return f"""\
airflow_bundle_metadata_version: "1.0"
sdk:
  language: typescript
  version: "0.1.0"
  supervisor_schema_version: "{schema_version}"
source: src/airflow.ts
dags:
  test_dag:
    tasks:
      - test_task
"""


def write_bundle(root: pathlib.Path, schema_version: str = SCHEMA_VERSION) -> pathlib.Path:
    bundle = root / "bundle.mjs"
    bundle.write_text("export {};\n", encoding="utf-8")
    (root / "airflow-metadata.yaml").write_text(_metadata_yaml(schema_version), encoding="utf-8")
    return bundle


def write_embedded_bundle(root: pathlib.Path, payload: str | None = None) -> pathlib.Path:
    if payload is None:
        payload = base64.b64encode(_metadata_yaml(SCHEMA_VERSION).encode("utf-8")).decode("ascii")
    bundle = root / "bundle.mjs"
    bundle.write_text(f"//# airflowMetadata={payload}\nexport {{}};\n", encoding="utf-8")
    return bundle


class TestNodeCoordinatorAttributes:
    def test_default_kwargs(self):
        coordinator = NodeCoordinator(bundles_root="/airflow/ts-bundles")

        assert coordinator.node_executable == "node"
        assert coordinator.bundles_root == [pathlib.Path("/airflow/ts-bundles")]
        assert coordinator.task_startup_timeout == 10.0

    def test_custom_kwargs(self):
        coordinator = NodeCoordinator(
            node_executable="/opt/node/bin/node",
            bundles_root=["/airflow/ts-bundles", "~/extra-bundles"],
            task_startup_timeout=30.0,
        )

        assert coordinator.node_executable == "/opt/node/bin/node"
        assert coordinator.bundles_root == [
            pathlib.Path("/airflow/ts-bundles"),
            pathlib.Path("~/extra-bundles").expanduser(),
        ]
        assert coordinator.task_startup_timeout == 30.0

    def test_bundles_root_is_required(self):
        with pytest.raises(ValueError, match="Length of 'bundles_root' must be >= 1"):
            NodeCoordinator(bundles_root=None)


class TestNodeCoordinatorBundleSelection:
    def test_find_bundle_returns_bundle_mjs(self, tmp_path):
        bundle = write_bundle(tmp_path)

        found = _find_bundle([tmp_path])

        assert found.path == bundle
        assert found.schema_version == SCHEMA_VERSION

    def test_find_bundle_reads_embedded_metadata_without_sidecar(self, tmp_path):
        bundle = write_embedded_bundle(tmp_path)

        found = _find_bundle([tmp_path])

        assert found.path == bundle
        assert found.schema_version == SCHEMA_VERSION

    def test_find_bundle_prefers_embedded_metadata_over_sidecar(self, tmp_path):
        bundle = write_embedded_bundle(tmp_path)
        (tmp_path / "airflow-metadata.yaml").write_text("[not-a-mapping]\n", encoding="utf-8")

        found = _find_bundle([tmp_path])

        assert found.path == bundle
        assert found.schema_version == SCHEMA_VERSION

    @pytest.mark.parametrize(
        ("payload", "message"),
        [
            ("not-base64!", "cannot parse embedded airflow metadata"),
            (base64.b64encode(b"[not-a-mapping]").decode("ascii"), "must contain a mapping"),
        ],
    )
    def test_find_bundle_rejects_invalid_embedded_metadata(self, tmp_path, payload, message):
        write_embedded_bundle(tmp_path, payload=payload)

        with pytest.raises(FileNotFoundError, match=message):
            _find_bundle([tmp_path])

    def test_find_bundle_rejects_oversized_embedded_metadata(self, tmp_path):
        write_embedded_bundle(tmp_path, payload="A" * EMBEDDED_METADATA_MAX_BYTES)

        with pytest.raises(FileNotFoundError, match="embedded airflow metadata exceeds"):
            _find_bundle([tmp_path])

    def test_find_bundle_rejects_empty_marker(self, tmp_path):
        (tmp_path / "bundle.mjs").write_text("//# airflowMetadata=\nexport {};\n", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="must contain a mapping"):
            _find_bundle([tmp_path])

    def test_find_bundle_checks_multiple_roots(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        bundle = write_bundle(second)

        found = _find_bundle([first, second])

        assert found.path == bundle
        assert found.schema_version == SCHEMA_VERSION

    def test_find_bundle_ignores_other_mjs_names(self, tmp_path):
        (tmp_path / "tasks.mjs").write_text("export {};\n")

        with pytest.raises(FileNotFoundError, match="Cannot find bundle.mjs"):
            _find_bundle([tmp_path])

    def test_find_bundle_rejects_bundle_without_metadata(self, tmp_path):
        (tmp_path / "bundle.mjs").write_text("export {};\n", encoding="utf-8")

        with pytest.raises(FileNotFoundError, match="missing airflow-metadata.yaml"):
            _find_bundle([tmp_path])

    @pytest.mark.parametrize(
        ("metadata", "message"),
        [
            ("[not-a-mapping]\n", "airflow-metadata.yaml must contain a mapping"),
            ("sdk: [not-a-mapping]\n", "missing sdk metadata mapping"),
            ("sdk:\n  language: typescript\n", "missing or invalid sdk.supervisor_schema_version"),
        ],
    )
    def test_find_bundle_rejects_invalid_metadata_shape(self, tmp_path, metadata, message):
        (tmp_path / "bundle.mjs").write_text("export {};\n", encoding="utf-8")
        (tmp_path / "airflow-metadata.yaml").write_text(metadata, encoding="utf-8")

        with pytest.raises(FileNotFoundError, match=message):
            _find_bundle([tmp_path])

    def test_find_bundle_reports_unreadable_metadata(self, tmp_path, monkeypatch):
        write_bundle(tmp_path)

        def raise_os_error(self, *args, **kwargs):
            if self.name == "airflow-metadata.yaml":
                raise PermissionError("denied")
            return original_open(self, *args, **kwargs)

        original_open = pathlib.Path.open
        monkeypatch.setattr(pathlib.Path, "open", raise_os_error)

        with pytest.raises(FileNotFoundError, match="cannot read airflow-metadata.yaml"):
            _find_bundle([tmp_path])

    def test_find_bundle_rejects_invalid_schema_version(self, tmp_path):
        write_bundle(tmp_path, schema_version="banana")

        with pytest.raises(FileNotFoundError, match="Version 'banana' not found"):
            _find_bundle([tmp_path])

    def test_find_bundle_skips_rejected_bundle_metadata(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        (first / "bundle.mjs").write_text("export {};\n", encoding="utf-8")
        bundle = write_bundle(second)

        found = _find_bundle([first, second])

        assert found.path == bundle
        assert found.schema_version == SCHEMA_VERSION

    def test_find_bundle_raises_with_searched_roots(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()

        with pytest.raises(FileNotFoundError) as exc_info:
            _find_bundle([first, second])

        msg = str(exc_info.value)
        assert str(first.resolve()) in msg
        assert str(second.resolve()) in msg


class TestNodeCoordinatorExecuteTaskCommand:
    def test_build_execute_task_command_returns_node_bundle_and_schema_version(self, tmp_path):
        bundle = write_bundle(tmp_path)
        coordinator = NodeCoordinator(
            node_executable="/opt/node/bin/node",
            bundles_root=tmp_path,
        )

        command, schema_version = coordinator._build_execute_task_command(what=_make_ti())

        assert command == ["/opt/node/bin/node", str(bundle)]
        assert schema_version == SCHEMA_VERSION
