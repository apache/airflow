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

import json
import pathlib
from unittest import mock

import pytest
from task_sdk.coordinators.node._bundle_test_utils import (
    mutate_byte,
    read_layout,
    write_bundle,
)
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.coordinators._bundle_metadata import ResolvedBundle
from airflow.sdk.coordinators.node import _bundle_reader as _reader
from airflow.sdk.coordinators.node._bundle_reader import _digest_cache
from airflow.sdk.coordinators.node.coordinator import NodeCoordinator, _select_bundle

SCHEMA_VERSION = "2026-06-16"


@pytest.fixture(autouse=True)
def clear_digest_cache():
    _digest_cache.clear()


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


class TestNodeCoordinatorExecuteTaskCommand:
    @mock.patch("airflow.sdk.coordinators.node.coordinator._select_bundle", autospec=True)
    def test_selects_bundle_by_dag_id(self, select_bundle, tmp_path):
        selected = tmp_path / "selected" / "bundle.mjs"
        select_bundle.return_value = ResolvedBundle(path=selected, schema_version=SCHEMA_VERSION)
        coordinator = NodeCoordinator(
            node_executable="/opt/node/bin/node",
            bundles_root=tmp_path,
        )

        command, schema_version = coordinator._build_execute_task_command(what=_make_ti(dag_id="sales"))

        select_bundle.assert_called_once_with([tmp_path], "sales")
        assert command == ["/opt/node/bin/node", str(selected)]
        assert schema_version == SCHEMA_VERSION


class TestBundleSelection:
    def test_ignores_roots_without_bundle_mjs(self, tmp_path):
        (tmp_path / "tasks.mjs").write_bytes(b"export {};\n")

        with pytest.raises(FileNotFoundError, match="dag_id='sales'"):
            _select_bundle([tmp_path], "sales")

    def test_reports_unreadable_bundle(self, tmp_path, monkeypatch):
        write_bundle(tmp_path, "sales")
        original_open = pathlib.Path.open

        def raise_os_error(self, *args, **kwargs):
            if self.name == "bundle.mjs":
                raise PermissionError("denied")
            return original_open(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "open", raise_os_error)

        with pytest.raises(FileNotFoundError, match="cannot read bundle.mjs"):
            _select_bundle([tmp_path], "sales")

    def test_skips_root_when_bundle_probe_fails(self, tmp_path, monkeypatch):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "sales")
        expected = write_bundle(second, "sales")
        original_is_file = pathlib.Path.is_file

        def fail_first_probe(self):
            if self.parent == first:
                raise PermissionError("denied")
            return original_is_file(self)

        monkeypatch.setattr(pathlib.Path, "is_file", fail_first_probe)

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_skips_rejected_bundle_when_path_resolution_fails(self, tmp_path, monkeypatch):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        (first / "bundle.mjs").write_bytes(b"export {};\n")
        expected = write_bundle(second, "sales")
        original_resolve = pathlib.Path.resolve

        def fail_first_resolve(self, *args, **kwargs):
            if self.parent == first:
                raise PermissionError("denied")
            return original_resolve(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "resolve", fail_first_resolve)

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_selects_later_bundle_containing_requested_dag(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "inventory")
        expected = write_bundle(second, "sales")

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_first_configured_match_wins_for_duplicate_dag(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        expected = write_bundle(first, "sales", code=b'console.log("first");\n')
        write_bundle(second, "sales", code=b'console.log("second");\n')

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_skips_corrupt_candidate_and_selects_later_match(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        corrupt = write_bundle(first, "sales")
        layout = read_layout(corrupt)
        mutate_byte(corrupt, int(layout["code"]["start"], 16))  # type: ignore[index, call-overload]
        expected = write_bundle(second, "sales")

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_skips_deeply_nested_metadata_and_selects_later_match(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        deeply_nested_json = b'{"nested":' + (b"[" * 10_000) + b"0" + (b"]" * 10_000) + b"}"
        write_bundle(first, "sales", metadata_payload=deeply_nested_json)
        expected = write_bundle(second, "sales")

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_skips_layout_decoder_recursion_and_selects_later_match(self, tmp_path, monkeypatch):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "sales")
        expected = write_bundle(second, "sales")
        original_loads = json.loads
        call_count = 0

        def recurse_once(payload):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RecursionError("test recursion")
            return original_loads(payload)

        monkeypatch.setattr(_reader.json, "loads", recurse_once)

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_skips_matching_bundle_with_invalid_schema_version(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        write_bundle(first, "sales", schema_version="banana")
        expected = write_bundle(second, "sales")

        found = _select_bundle([first, second], "sales")

        assert found.path == expected

    def test_error_names_dag_roots_and_rejected_candidates(self, tmp_path):
        first = tmp_path / "first"
        second = tmp_path / "second"
        first.mkdir()
        second.mkdir()
        (first / "bundle.mjs").write_bytes(b"export {};\n")
        write_bundle(second, "inventory")

        with pytest.raises(FileNotFoundError) as exc_info:
            _select_bundle([first, second], "sales")

        message = str(exc_info.value)
        assert "dag_id='sales'" in message
        assert str(first) in message
        assert str(second) in message
        assert "rejected candidates" in message
        assert "matching bundles were rejected" not in message
