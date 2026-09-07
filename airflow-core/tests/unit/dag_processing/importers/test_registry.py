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
"""Tests for the DagImporterRegistry."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from airflow.dag_processing.importers import (
    DagImporterRegistry,
    PythonDagImporter,
    get_importer_registry,
    reset_importer_registry,
)
from airflow.exceptions import AirflowConfigException

from tests_common.test_utils.config import conf_vars


class GlobalDagImporter(PythonDagImporter):
    pass


class BundleDagImporter(PythonDagImporter):
    pass


class LazyTestImporter(PythonDagImporter):
    instances = 0

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        LazyTestImporter.instances += 1


class TestDagImporterRegistry:
    """Test the DagImporterRegistry."""

    def test_default_importers_registered(self):
        """Registry should have Python importer by default."""
        registry = get_importer_registry()
        extensions = registry.supported_extensions()
        assert ".py" in extensions

    def test_get_importer_for_python(self):
        """Should return PythonDagImporter for .py files."""
        registry = get_importer_registry()
        importer = registry.get_importer("test.py")
        assert importer is not None
        assert isinstance(importer, PythonDagImporter)

    def test_get_importer_for_unknown(self):
        """Should return None for unknown file types."""
        registry = get_importer_registry()
        assert registry.get_importer("test.txt") is None

    def test_can_handle_supported_files(self):
        """can_handle should return True for supported file types."""
        registry = get_importer_registry()
        assert registry.can_handle("dag.py")
        assert registry.can_handle(Path("subdir/dag.py"))

    def test_can_handle_unsupported_files(self):
        """can_handle should return False for unsupported file types."""
        registry = get_importer_registry()
        assert not registry.can_handle("readme.txt")
        assert not registry.can_handle("config.json")
        assert not registry.can_handle("script.sh")

    def test_case_insensitive_extension_matching(self):
        """Extension matching should be case-insensitive."""
        registry = get_importer_registry()
        assert registry.can_handle("dag.PY")
        assert registry.can_handle("dag.Py")

    def test_reset_clears_singleton(self):
        """reset() should clear the singleton instance."""
        registry1 = get_importer_registry()
        DagImporterRegistry.reset()
        registry2 = get_importer_registry()
        assert registry1 is not registry2

    def test_independent_registry_instances(self):
        """Directly instantiating DagImporterRegistry creates isolated instances."""
        assert DagImporterRegistry() is not DagImporterRegistry()

    def test_register_explicit_extensions_and_override_warning(self, caplog):
        """register() with explicit extensions overrides existing and logs warning."""
        reg = DagImporterRegistry(register_defaults=True)
        importer = PythonDagImporter()
        with caplog.at_level(logging.WARNING):
            reg.register(importer, extensions=[".py", "custom"])
        assert reg.can_handle("test.custom")
        assert importer.can_handle("test.custom")
        assert any("Extension '.py' already registered" in r.message for r in caplog.records)

    def test_list_dag_files_with_configured_extensions(self, tmp_path):
        """list_dag_files() discovers files matching explicitly configured extensions."""
        dag_file = tmp_path / "valid_dag.custom"
        dag_file.write_text("from airflow.sdk import DAG\n")
        ignored_file = tmp_path / "ignored.py"
        ignored_file.write_text("from airflow.sdk import DAG\n")
        notes_file = tmp_path / "notes.txt"
        notes_file.write_text("not a dag")

        reg = DagImporterRegistry(register_defaults=False)
        reg.register(PythonDagImporter(), extensions=[".custom"])

        files = reg.list_dag_files(tmp_path, safe_mode=True)
        assert files == [str(dag_file)]

    def test_list_dag_files_single_file(self, tmp_path):
        """list_dag_files() returns single file if registered importer can handle it."""
        dag_file = tmp_path / "valid_dag.custom"
        dag_file.write_text("from airflow.sdk import DAG\n")

        reg = DagImporterRegistry(register_defaults=False)
        reg.register(PythonDagImporter(), extensions=[".custom"])

        assert reg.list_dag_files(dag_file, safe_mode=True) == [str(dag_file)]

        unhandled_file = tmp_path / "unhandled.txt"
        unhandled_file.write_text("text")
        assert reg.list_dag_files(unhandled_file, safe_mode=True) == []

    def test_list_dag_files_respects_safe_mode(self, tmp_path):
        """list_dag_files() filters non-Dag files when safe_mode=True."""
        no_keywords_file = tmp_path / "no_keywords.custom"
        no_keywords_file.write_text("print('hello world')\n")

        reg = DagImporterRegistry(register_defaults=False)
        reg.register(PythonDagImporter(), extensions=[".custom"])

        assert reg.list_dag_files(tmp_path, safe_mode=True) == []
        assert reg.list_dag_files(tmp_path, safe_mode=False) == [str(no_keywords_file)]

    def test_list_dag_files_single_pass_override(self, tmp_path):
        """list_dag_files() routes discovered files to overriding importer."""
        py_file = tmp_path / "dag.py"
        py_file.write_text("from airflow.sdk import DAG\n")

        class OverridingImporter(PythonDagImporter):
            pass

        reg = DagImporterRegistry(register_defaults=True)
        overriding_importer = OverridingImporter()
        reg.register(overriding_importer, extensions=[".py"])

        files = reg.list_dag_files(tmp_path, safe_mode=True)
        assert files == [str(py_file)]
        assert reg.get_importer(py_file) is overriding_importer

    def test_lazy_importer_instantiation(self):
        """Importer classes are not imported or instantiated until get_importer is called."""
        LazyTestImporter.instances = 0
        reg = DagImporterRegistry(register_defaults=False)
        reg.register_specs(
            [
                {
                    "classpath": f"{__name__}.LazyTestImporter",
                    "extensions": [".lazy", ".lazy2"],
                    "kwargs": {"param": "value"},
                }
            ],
            context="test",
        )

        assert LazyTestImporter.instances == 0
        assert reg.can_handle("file.lazy")
        assert reg.can_handle("file.lazy2")
        assert LazyTestImporter.instances == 0
        assert set(reg.supported_extensions()) == {".lazy", ".lazy2"}
        assert LazyTestImporter.instances == 0

        importer1 = reg.get_importer("file.lazy")
        assert LazyTestImporter.instances == 1
        assert isinstance(importer1, LazyTestImporter)
        assert importer1.kwargs == {"param": "value"}

        importer2 = reg.get_importer("file.lazy2")
        assert importer2 is importer1
        assert LazyTestImporter.instances == 1

    def test_from_config_three_tier_precedence(self):
        """Bundle config overrides global config, which overrides defaults."""
        global_config = [
            {
                "classpath": f"{__name__}.GlobalDagImporter",
                "extensions": [".py", ".custom"],
            }
        ]
        bundle_config_list = [
            {
                "name": "test_bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": "/tmp/test"},
                "importers": [
                    {
                        "classpath": f"{__name__}.BundleDagImporter",
                        "extensions": [".custom"],
                    }
                ],
            }
        ]

        with conf_vars(
            {
                ("dag_processor", "dag_importer_configs"): json.dumps(global_config),
                ("dag_processor", "dag_bundle_config_list"): json.dumps(bundle_config_list),
            }
        ):
            global_reg = DagImporterRegistry.from_config()
            assert isinstance(global_reg.get_importer("dag.py"), GlobalDagImporter)
            assert isinstance(global_reg.get_importer("dag.custom"), GlobalDagImporter)

            bundle_reg = DagImporterRegistry.from_config("test_bundle")
            assert isinstance(bundle_reg.get_importer("dag.py"), GlobalDagImporter)
            assert isinstance(bundle_reg.get_importer("dag.custom"), BundleDagImporter)

    @pytest.mark.parametrize(
        ("global_cfg", "bundle_cfg", "match"),
        [
            (json.dumps({"invalid": "object"}), None, "key `dag_importer_configs` must be a list"),
            (None, [{"extensions": [".py"]}], "Missing required 'classpath'"),
            (None, [{"classpath": "invalid.path"}], "Failed to load DAG importer"),
            (
                None,
                [{"classpath": "builtins.dict"}],
                r"Configured DAG importer builtins\.dict for bundle 'test_bundle' must inherit from AbstractDagImporter\.",
            ),
            (
                json.dumps([{"classpath": "builtins.dict"}]),
                None,
                r"Configured DAG importer builtins\.dict for global configuration must inherit from AbstractDagImporter\.",
            ),
        ],
    )
    def test_from_config_invalid_configs(self, global_cfg, bundle_cfg, match):
        """Invalid configurations raise AirflowConfigException."""
        overrides = {}
        if global_cfg:
            overrides[("dag_processor", "dag_importer_configs")] = global_cfg
        if bundle_cfg:
            overrides[("dag_processor", "dag_bundle_config_list")] = json.dumps(
                [
                    {
                        "name": "test_bundle",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": "/tmp/test"},
                        "importers": bundle_cfg,
                    }
                ]
            )

        with conf_vars(overrides), pytest.raises(AirflowConfigException, match=match):
            DagImporterRegistry.from_config("test_bundle" if bundle_cfg else None)

    def test_get_importer_registry_caching_and_isolation(self):
        """get_importer_registry caches instances per bundle name and clears on reset."""
        reg1 = get_importer_registry()
        reg2 = get_importer_registry()
        assert reg1 is reg2

        bundle_a = get_importer_registry("bundle_a")
        bundle_a2 = get_importer_registry("bundle_a")
        bundle_b = get_importer_registry("bundle_b")

        assert bundle_a is bundle_a2
        assert bundle_a is not bundle_b
        assert bundle_a is not reg1

        reset_importer_registry()
        new_reg = get_importer_registry()
        new_bundle_a = get_importer_registry("bundle_a")
        assert new_reg is not reg1
        assert new_bundle_a is not bundle_a
