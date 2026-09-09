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

from airflow.sdk.exceptions import AirflowConfigException
from airflow.sdk.importers import (
    AbstractDagImporter,
    DagDefinition,
    DagImporterRegistry,
    DagImportResult,
    DagSourceCode,
    FileDagDefinition,
    PythonDagImporter,
    ZipFileDagDefinition,
    ZipImporter,
    get_file_suffix,
    get_importer_registry,
    reset_importer_registry,
)

from tests_common.test_utils.config import conf_vars


class GlobalDagImporter(PythonDagImporter):
    pass


class BundleDagImporter(PythonDagImporter):
    pass


class CustomBundleNonExtensionImporter(AbstractDagImporter):
    """A non-extension custom importer that routes by URI prefix."""

    def can_handle(self, definition: DagDefinition | str | Path) -> bool:
        return isinstance(definition, str) and definition.startswith("custom://")

    def list_dag_definitions(self, bundle, **kwargs):
        return iter([])

    def import_definition(self, definition, bundle=None, **kwargs):
        return DagImportResult()

    def get_source_code(self, definition):
        return DagSourceCode(source_code="", language="text")


class LazyTestImporter(PythonDagImporter):
    instances = 0

    def __init__(self, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        LazyTestImporter.instances += 1


class TestDagImporterRegistry:
    """Test the DagImporterRegistry."""

    @pytest.fixture(autouse=True)
    def _clean_registry(self):
        """Reset the registry before and after each test."""
        reset_importer_registry()
        yield
        reset_importer_registry()

    def test_singleton_pattern(self):
        """Registry should return the same instance."""
        registry1 = get_importer_registry()
        registry2 = get_importer_registry()
        assert registry1 is registry2

    def test_default_importers_registered(self):
        """Registry should have Python importer by default."""
        registry = get_importer_registry()
        extensions = registry.supported_extensions()

        assert ".py" in extensions
        assert ".zip" in extensions

    def test_get_importer_for_python(self):
        """Should return PythonDagImporter for .py files."""
        registry = get_importer_registry()
        importer = registry.get_importer("test.py")

        assert importer is not None
        assert isinstance(importer, PythonDagImporter)

    def test_get_importer_for_zip(self):
        """Should return ZipImporter for .zip files."""
        registry = get_importer_registry()
        importer = registry.get_importer("test.zip")

        assert importer is not None
        assert isinstance(importer, ZipImporter)

    def test_get_importer_for_unknown(self):
        """Should return None for unknown file types."""
        registry = get_importer_registry()
        importer = registry.get_importer("test.txt")

        assert importer is None

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

        # All these should be handled
        assert registry.can_handle("dag.PY")
        assert registry.can_handle("dag.Py")

    def test_reset_clears_singleton(self):
        """reset() should clear the singleton instance."""
        registry1 = get_importer_registry()
        DagImporterRegistry.reset()
        registry2 = get_importer_registry()

        # Should be different instances after reset
        assert registry1 is not registry2

    def test_custom_importer_without_extensions(self):
        """Registry should resolve non-file-based importers via can_handle."""
        registry = get_importer_registry()
        importer = CustomBundleNonExtensionImporter()
        registry.register(importer)

        assert registry.can_handle("custom://dags/sample")
        assert registry.get_importer("custom://dags/sample") is importer
        assert not registry.can_handle("other://dags/sample")

    def test_abstract_dag_importer_has_no_extension_attributes_or_methods(self):
        """AbstractDagImporter must not define file extension attributes or methods."""
        assert not hasattr(AbstractDagImporter, "supported_extensions")
        assert not hasattr(AbstractDagImporter, "might_contain_dag")

    def test_custom_importer_file_pattern_can_handle(self):
        """Registry resolves file definitions via can_handle when importer has no supported_extensions."""

        class PatternDagImporter(PythonDagImporter):
            def can_handle(self, definition: DagDefinition | str | Path) -> bool:
                return "special_dag_" in str(definition) and str(definition).endswith(".json")

        registry = get_importer_registry()
        importer = PatternDagImporter()
        registry.register(importer)

        assert registry.can_handle("special_dag_1.json")
        assert registry.get_importer("special_dag_1.json") is importer
        assert not registry.can_handle("normal_dag.json")
        assert registry.get_importer("normal_dag.json") is None

    def test_override_importer_for_extension(self, caplog):
        """Registering an importer for an existing extension logs a warning and evicts the previous importer."""
        registry = DagImporterRegistry(register_defaults=True)
        custom_importer = GlobalDagImporter()

        with caplog.at_level(logging.WARNING):
            registry.register(custom_importer, extensions=[".py"])

        assert registry.get_importer("test.py") is custom_importer
        assert any(
            record.levelno == logging.WARNING and "already registered" in record.message
            for record in caplog.records
        )

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

    def test_bundle_config_with_extensions_and_non_extension_importers(self):
        """Verify bundle config with extensions, non-extension importers, and archive importer."""
        bundle_config_list = [
            {
                "name": "dags-folder",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {},
                "importers": [
                    {
                        "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                        "extensions": [".py"],
                    },
                    {
                        "classpath": f"{__name__}.CustomBundleNonExtensionImporter",
                    },
                    {
                        "classpath": "airflow.sdk.importers.zip_importer.ZipImporter",
                        "extensions": [".zip"],
                        "kwargs": {
                            "internal_importers": [
                                {
                                    "classpath": "airflow.sdk.importers.python_importer.PythonDagImporter",
                                    "extensions": [".py"],
                                }
                            ]
                        },
                    },
                ],
            }
        ]

        with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(bundle_config_list)}):
            registry = DagImporterRegistry.from_config("dags-folder")

            # 1. Importer with extensions (.py)
            assert registry.can_handle("sample_dag.py")
            py_importer = registry.get_importer("sample_dag.py")
            assert isinstance(py_importer, PythonDagImporter)

            # 2. Non-extension importer (custom://)
            assert registry.can_handle("custom://my-pipeline-1")
            custom_importer = registry.get_importer("custom://my-pipeline-1")
            assert isinstance(custom_importer, CustomBundleNonExtensionImporter)

            # 3. Archive importer (.zip) with internal_importers
            assert registry.can_handle("bundle.zip")
            zip_importer = registry.get_importer("bundle.zip")
            assert isinstance(zip_importer, ZipImporter)
            assert ".py" in zip_importer._internal_extension_importers

    @pytest.mark.parametrize(
        ("input_val", "expected"),
        [
            ("foo.py", ".py"),
            ("path/to/FOO.PY", ".py"),
            (Path("archive.ZIP"), ".zip"),
            ("no_extension", ""),
            (FileDagDefinition(path=Path("my_dag.py")), ".py"),
            (ZipFileDagDefinition(zip_path=Path("a.zip"), file_path="nested/workflow.py"), ".py"),
            (None, None),
        ],
    )
    def test_get_file_suffix(self, input_val, expected):
        assert get_file_suffix(input_val) == expected
