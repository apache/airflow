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

from pathlib import Path

from airflow.dag_processing.importers import (
    DagImporterRegistry,
    PythonDagImporter,
    get_importer_registry,
)


class TestDagImporterRegistry:
    """Test the DagImporterRegistry singleton."""

    def setup_method(self):
        """Reset the registry before each test."""
        DagImporterRegistry.reset()

    def teardown_method(self):
        """Reset the registry after each test."""
        DagImporterRegistry.reset()

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

    def test_get_importer_for_python(self):
        """Should return PythonDagImporter for .py files."""
        registry = get_importer_registry()
        importer = registry.get_importer("test.py")

        assert importer is not None
        assert isinstance(importer, PythonDagImporter)

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
