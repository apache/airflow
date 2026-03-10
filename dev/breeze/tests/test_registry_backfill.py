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
"""Unit tests for the registry backfill command helpers."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from airflow_breeze.commands.registry_commands import (
    _build_pip_spec,
    _ensure_providers_json,
    _find_provider_yaml,
    _patch_providers_json,
    _read_provider_yaml_info,
)


# ---------------------------------------------------------------------------
# _find_provider_yaml
# ---------------------------------------------------------------------------
class TestFindProviderYaml:
    def test_simple_provider(self):
        path = _find_provider_yaml("amazon")
        assert path.name == "provider.yaml"
        assert "providers/amazon" in str(path)

    def test_hyphenated_provider(self):
        path = _find_provider_yaml("microsoft-azure")
        assert path.name == "provider.yaml"
        assert "providers/microsoft/azure" in str(path)

    def test_triple_hyphenated_provider(self):
        path = _find_provider_yaml("apache-beam")
        assert path.name == "provider.yaml"
        assert "providers/apache/beam" in str(path) or "providers/apache-beam" in str(path)

    def test_unknown_provider_raises(self):
        with pytest.raises(Exception, match="provider.yaml not found"):
            _find_provider_yaml("nonexistent-provider-xyz")


# ---------------------------------------------------------------------------
# _read_provider_yaml_info
# ---------------------------------------------------------------------------
class TestReadProviderYamlInfo:
    def test_reads_package_name_and_extras(self, tmp_path):
        provider_dir = tmp_path / "providers" / "amazon"
        provider_dir.mkdir(parents=True)
        (provider_dir / "provider.yaml").write_text("package-name: apache-airflow-providers-amazon\n")
        (provider_dir / "pyproject.toml").write_text(
            '[project.optional-dependencies]\npandas = ["pandas>=2.1.2"]\ns3fs = ["s3fs>=2024.6.1"]\n'
        )
        with patch("airflow_breeze.commands.registry_commands.PROVIDERS_DIR", tmp_path / "providers"):
            package_name, extras = _read_provider_yaml_info("amazon")
        assert package_name == "apache-airflow-providers-amazon"
        assert extras == ["pandas", "s3fs"]

    def test_no_pyproject_returns_empty_extras(self, tmp_path):
        provider_dir = tmp_path / "providers" / "ftp"
        provider_dir.mkdir(parents=True)
        (provider_dir / "provider.yaml").write_text("package-name: apache-airflow-providers-ftp\n")
        with patch("airflow_breeze.commands.registry_commands.PROVIDERS_DIR", tmp_path / "providers"):
            package_name, extras = _read_provider_yaml_info("ftp")
        assert package_name == "apache-airflow-providers-ftp"
        assert extras == []

    def test_pyproject_without_optional_deps(self, tmp_path):
        provider_dir = tmp_path / "providers" / "sqlite"
        provider_dir.mkdir(parents=True)
        (provider_dir / "provider.yaml").write_text("package-name: apache-airflow-providers-sqlite\n")
        (provider_dir / "pyproject.toml").write_text("[project]\nname = 'test'\n")
        with patch("airflow_breeze.commands.registry_commands.PROVIDERS_DIR", tmp_path / "providers"):
            _, extras = _read_provider_yaml_info("sqlite")
        assert extras == []


# ---------------------------------------------------------------------------
# _build_pip_spec
# ---------------------------------------------------------------------------
class TestBuildPipSpec:
    def test_with_extras(self):
        result = _build_pip_spec("apache-airflow-providers-amazon", ["pandas", "s3fs"], "9.21.0")
        assert result == "apache-airflow-providers-amazon[pandas,s3fs]==9.21.0"

    def test_without_extras(self):
        result = _build_pip_spec("apache-airflow-providers-ftp", [], "1.0.0")
        assert result == "apache-airflow-providers-ftp==1.0.0"

    def test_single_extra(self):
        result = _build_pip_spec("apache-airflow-providers-google", ["leveldb"], "10.0.0")
        assert result == "apache-airflow-providers-google[leveldb]==10.0.0"


# ---------------------------------------------------------------------------
# _ensure_providers_json
# ---------------------------------------------------------------------------
class TestEnsureProvidersJson:
    def test_creates_new_file(self, tmp_path):
        providers_json = tmp_path / "dev" / "registry" / "providers.json"
        with patch(
            "airflow_breeze.commands.registry_commands.PROVIDERS_JSON_PATH",
            providers_json,
        ):
            result = _ensure_providers_json("amazon", "apache-airflow-providers-amazon")

        assert result == providers_json
        data = json.loads(providers_json.read_text())
        assert len(data["providers"]) == 1
        assert data["providers"][0]["id"] == "amazon"
        assert data["providers"][0]["package_name"] == "apache-airflow-providers-amazon"

    def test_appends_to_existing_file(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        providers_json.write_text(
            json.dumps({"providers": [{"id": "google", "package_name": "pkg-google", "version": "1.0.0"}]})
        )
        with patch(
            "airflow_breeze.commands.registry_commands.PROVIDERS_JSON_PATH",
            providers_json,
        ):
            _ensure_providers_json("amazon", "apache-airflow-providers-amazon")

        data = json.loads(providers_json.read_text())
        assert len(data["providers"]) == 2
        ids = [p["id"] for p in data["providers"]]
        assert "google" in ids
        assert "amazon" in ids

    def test_skips_if_provider_already_present(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        original = {"providers": [{"id": "amazon", "package_name": "pkg", "version": "1.0.0"}]}
        providers_json.write_text(json.dumps(original))
        with patch(
            "airflow_breeze.commands.registry_commands.PROVIDERS_JSON_PATH",
            providers_json,
        ):
            _ensure_providers_json("amazon", "pkg")

        # File should be unchanged
        data = json.loads(providers_json.read_text())
        assert len(data["providers"]) == 1


# ---------------------------------------------------------------------------
# _patch_providers_json
# ---------------------------------------------------------------------------
class TestPatchProvidersJson:
    def test_patches_version(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        providers_json.write_text(json.dumps({"providers": [{"id": "amazon", "version": "9.22.0"}]}))
        original = _patch_providers_json(providers_json, "amazon", "9.15.0")
        assert original == "9.22.0"

        data = json.loads(providers_json.read_text())
        assert data["providers"][0]["version"] == "9.15.0"

    def test_raises_for_missing_provider(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        providers_json.write_text(json.dumps({"providers": [{"id": "google", "version": "1.0.0"}]}))
        with pytest.raises(Exception, match="not found"):
            _patch_providers_json(providers_json, "amazon", "9.15.0")

    def test_restores_original_version(self, tmp_path):
        providers_json = tmp_path / "providers.json"
        providers_json.write_text(json.dumps({"providers": [{"id": "amazon", "version": "9.22.0"}]}))
        # Patch to target version
        _patch_providers_json(providers_json, "amazon", "9.15.0")
        # Restore
        _patch_providers_json(providers_json, "amazon", "9.22.0")

        data = json.loads(providers_json.read_text())
        assert data["providers"][0]["version"] == "9.22.0"
