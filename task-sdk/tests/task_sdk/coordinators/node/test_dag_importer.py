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

import pathlib
from types import SimpleNamespace

import pytest
from task_sdk.coordinators.node._bundle_test_utils import (
    BUNDLE_NAME,
    LAYOUT_PREFIX,
    SOURCE_OPEN,
    mutate_byte,
    mutate_section,
    write_bundle,
)

from airflow.sdk.coordinators.node._bundle_reader import _digest_cache
from airflow.sdk.coordinators.node._dag_importer import NodeDagImporter
from airflow.sdk.coordinators.node.coordinator import NodeCoordinator
from airflow.sdk.importers import DagSourceCode, FilesystemDagDefinition


@pytest.fixture(autouse=True)
def clear_digest_cache():
    _digest_cache.clear()


@pytest.fixture
def importer() -> NodeDagImporter:
    return NodeDagImporter(coordinator=NodeCoordinator())


def test_lists_only_packed_bundles(importer, tmp_path):
    nested = write_bundle(tmp_path / "team", "sales")
    tampered = write_bundle(tmp_path, "inventory", name="tampered.min.mjs")
    mutate_section(tampered, "code")
    write_bundle(tmp_path, "orders", name="plain.mjs")
    (tmp_path / "vendor.min.mjs").write_bytes(b"export {};\n")

    definitions = importer.list_dag_definitions(SimpleNamespace(name="dags-folder", path=tmp_path))

    # A tampered bundle keeps its header, so parsing it reports the integrity failure.
    assert sorted(d.path for d in definitions) == sorted([nested, tampered])


class TestMightContainDag:
    @pytest.mark.parametrize("safe_mode", [True, False])
    @pytest.mark.parametrize(
        ("content", "expected"),
        [
            (None, True),
            (b"export {};\n", False),
            (b"", False),
            (LAYOUT_PREFIX[:-1], False),
        ],
        ids=["bundle", "plain-module", "empty", "truncated-prefix"],
    )
    def test_checks_the_layout_header(self, importer, tmp_path, content, expected, safe_mode):
        path = write_bundle(tmp_path, "sales")
        if content is not None:
            path.write_bytes(content)

        assert importer.might_contain_dag(FilesystemDagDefinition(path), safe_mode) is expected

    def test_keeps_an_unreadable_file(self, importer, tmp_path, monkeypatch):
        path = write_bundle(tmp_path, "sales")
        original_open = pathlib.Path.open

        def raise_permission_error(self, *args, **kwargs):
            if self.name == BUNDLE_NAME:
                raise PermissionError("denied")
            return original_open(self, *args, **kwargs)

        monkeypatch.setattr(pathlib.Path, "open", raise_permission_error)

        assert importer.might_contain_dag(FilesystemDagDefinition(path), True) is True


class TestGetSourceCode:
    def test_returns_the_entrypoint_source_for_every_dag(self, importer, tmp_path):
        sales = '/* the */ export const sales = new Dag({ dagId: "sales" });\n'
        inventory = 'export const inventory = new Dag({ dagId: "inventory" });\n'
        path = write_bundle(
            tmp_path,
            "sales",
            "inventory",
            sources=[("sales.ts", sales.encode()), ("inventory.ts", inventory.encode())],
            dag_source_paths={"sales": "sales.ts", "inventory": "inventory.ts"},
            entrypoint_path="sales.ts",
        )

        assert importer.get_source_code(FilesystemDagDefinition(path)) == DagSourceCode(sales, "typescript")

    def test_returns_a_notice_without_an_entrypoint_source(self, importer, tmp_path):
        path = write_bundle(tmp_path, "sales", entrypoint_path=None)

        source_code = importer.get_source_code(FilesystemDagDefinition(path))

        assert source_code == DagSourceCode(
            "// Source code is not available: the bundle embeds no entrypoint source.\n", "typescript"
        )

    @pytest.mark.parametrize(
        ("break_bundle", "reason"),
        [
            (
                lambda path: mutate_byte(path, path.read_bytes().index(SOURCE_OPEN) + len(SOURCE_OPEN)),
                "source main.ts SHA-256 mismatch",
            ),
            (lambda path: write_bundle(path.parent, "sales", source=b"x" * (1024 * 1024 + 1)), "exceeds"),
            (lambda path: path.unlink(), "cannot read bundle.min.mjs"),
        ],
        ids=["tampered", "oversize", "missing"],
    )
    def test_raises_when_the_source_cannot_be_read(self, importer, tmp_path, break_bundle, reason):
        path = write_bundle(tmp_path, "sales")
        break_bundle(path)

        with pytest.raises((OSError, ValueError), match=reason):
            importer.get_source_code(FilesystemDagDefinition(path))
