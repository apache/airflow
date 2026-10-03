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

from airflow.sdk.coordinators._dag_importer import find_claiming_importer
from airflow.sdk.coordinators.java._dag_importer import JavaDagImporter
from airflow.sdk.coordinators.node import NodeCoordinator
from airflow.sdk.coordinators.node._bundle_reader import _digest_cache
from airflow.sdk.coordinators.node._dag_importer import NodeDagImporter
from airflow.sdk.execution_time.coordinator import InvalidCoordinatorError
from airflow.sdk.importers import (
    DagSourceCode,
    FilesystemDagDefinition,
    get_importer_registry,
    reset_importer_registry,
)

from tests_common.test_utils.config import conf_vars

NODE_COORDINATOR = "airflow.sdk.coordinators.node.NodeCoordinator"


class _NodeCoordinatorSubclass(NodeCoordinator):
    pass


def _coordinators(*kwargs: dict, mapping: dict[str, str] | None = None) -> dict[tuple[str, str], str]:
    """Config with one NodeCoordinator per *kwargs*, keyed "ts-0", "ts-1" and so on."""
    specs = {f"ts-{i}": {"classpath": NODE_COORDINATOR, "kwargs": kw} for i, kw in enumerate(kwargs)}
    config = {("sdk", "coordinators"): json.dumps(specs)}
    if mapping is not None:
        config[("sdk", "dag_bundle_to_coordinator")] = json.dumps(mapping)
    return config


@pytest.fixture(autouse=True)
def clear_digest_cache():
    _digest_cache.clear()


@pytest.fixture(autouse=True)
def _clean_registry():
    reset_importer_registry()
    yield
    reset_importer_registry()


@pytest.fixture
def importer() -> NodeDagImporter:
    return NodeDagImporter(bundle_name="dags-folder")


def test_claims_packed_bundles_only(importer):
    assert importer.supported_extensions == [".mjs"]
    assert importer.can_handle("dags/bundle.min.mjs") is True
    assert importer.can_handle("dags/helper.mjs") is False


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
        sales = 'export const sales = new Dag({ dagId: "sales" });\n'
        inventory = 'export const inventory = new Dag({ dagId: "inventory" });\n'
        main = '/* the */ import "./sales";\nimport "./inventory";\n'
        path = write_bundle(
            tmp_path,
            "sales",
            "inventory",
            sources=[
                ("sales.ts", sales.encode()),
                ("inventory.ts", inventory.encode()),
                ("main.ts", main.encode()),
            ],
            dag_source_paths={"sales": "sales.ts", "inventory": "inventory.ts"},
            entrypoint_path="main.ts",
        )

        assert importer.get_source_code(FilesystemDagDefinition(path)) == DagSourceCode(main, "typescript")

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


class TestRegistry:
    def test_a_node_coordinator_registers_the_importer_in_every_bundle(self):
        bundles = [
            {"name": name, "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}}
            for name in ("dags-folder", "ts-bundles")
        ]
        coordinators = _coordinators({"task_handler_bundle_name": "ts-bundles"})
        with conf_vars({**coordinators, ("dag_processor", "dag_bundle_config_list"): json.dumps(bundles)}):
            in_named = get_importer_registry("ts-bundles").get_importer("ts-bundles/bundle.min.mjs")
            in_other = get_importer_registry("dags-folder").get_importer("dags/bundle.min.mjs")

        assert isinstance(in_named, NodeDagImporter)
        assert isinstance(in_other, NodeDagImporter)
        assert in_other.bundle_name == "dags-folder"

    def test_a_subclass_of_the_node_coordinator_registers_the_importer(self):
        subclass = {"ts": {"classpath": f"{__name__}._NodeCoordinatorSubclass"}}
        with conf_vars({("sdk", "coordinators"): json.dumps(subclass)}):
            claiming = find_claiming_importer("dags/bundle.min.mjs", "dags-folder")

        assert isinstance(claiming, NodeDagImporter)

    def test_a_bundle_with_java_and_node_coordinators_sends_each_file_to_its_runtime(self):
        coordinators = {
            "java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"},
            "ts": {"classpath": NODE_COORDINATOR},
        }
        with conf_vars({("sdk", "coordinators"): json.dumps(coordinators)}):
            jar = find_claiming_importer("dags/app.jar", "dags-folder")
            bundle = find_claiming_importer("dags/bundle.min.mjs", "dags-folder")

        assert isinstance(jar, JavaDagImporter)
        assert isinstance(bundle, NodeDagImporter)


class TestGetParsingCoordinator:
    def test_is_the_only_node_coordinator(self, importer):
        with conf_vars(_coordinators({"node_executable": "/opt/node/bin/node"})):
            coordinator = importer.get_parsing_coordinator()

        assert isinstance(coordinator, NodeCoordinator)
        assert coordinator.node_executable == "/opt/node/bin/node"

    @pytest.mark.parametrize(("mapped", "executable"), [("ts-0", "/node/18"), ("ts-1", "/node/22")])
    def test_is_the_node_coordinator_mapped_to_its_bundle_among_several(self, importer, mapped, executable):
        coordinators = _coordinators(
            {"node_executable": "/node/18"},
            {"node_executable": "/node/22"},
            mapping={"dags-folder": mapped},
        )
        with conf_vars(coordinators):
            coordinator = importer.get_parsing_coordinator()

        assert coordinator.node_executable == executable

    def test_fails_among_several_without_an_entry_for_its_bundle(self, importer):
        with conf_vars(_coordinators({}, {})):
            with pytest.raises(
                InvalidCoordinatorError, match="Dag bundle 'dags-folder' has 2 NodeCoordinator"
            ):
                importer.get_parsing_coordinator()
