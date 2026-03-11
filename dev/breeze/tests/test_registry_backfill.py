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
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.commands.registry_commands import (
    _build_pip_spec,
    _create_isolated_providers_json,
    _find_provider_yaml,
    _read_provider_yaml_info,
    _run_extract_script,
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
# _create_isolated_providers_json
# ---------------------------------------------------------------------------
class TestCreateIsolatedProvidersJson:
    def test_creates_file_with_correct_content(self, tmp_path):
        result = _create_isolated_providers_json(
            "amazon", "apache-airflow-providers-amazon", "9.15.0", tmp_path
        )

        assert result.exists()
        data = json.loads(result.read_text())
        assert len(data["providers"]) == 1
        assert data["providers"][0]["id"] == "amazon"
        assert data["providers"][0]["package_name"] == "apache-airflow-providers-amazon"
        assert data["providers"][0]["version"] == "9.15.0"

    def test_filename_includes_provider_and_version(self, tmp_path):
        result = _create_isolated_providers_json("google", "pkg", "14.0.0", tmp_path)
        assert result.name == "providers-google-14.0.0.json"

    def test_different_versions_produce_different_files(self, tmp_path):
        f1 = _create_isolated_providers_json("amazon", "pkg", "9.15.0", tmp_path)
        f2 = _create_isolated_providers_json("amazon", "pkg", "9.14.0", tmp_path)
        assert f1 != f2
        assert f1.exists()
        assert f2.exists()


# ---------------------------------------------------------------------------
# _run_extract_script
# ---------------------------------------------------------------------------
class TestRunExtractScript:
    def test_success_on_first_try(self, tmp_path):
        script = tmp_path / "extract.py"
        providers_json = tmp_path / "providers.json"

        mock_result = MagicMock(returncode=0)
        with patch(
            "airflow_breeze.commands.registry_commands.run_command", return_value=mock_result
        ) as mock_run:
            rc = _run_extract_script(script, "pkg[extra]==1.0", "pkg==1.0", "amazon", providers_json)

        assert rc == 0
        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        assert "--provider" in cmd
        assert "amazon" in cmd
        assert "--providers-json" in cmd

    def test_falls_back_without_extras_on_failure(self, tmp_path):
        script = tmp_path / "extract.py"
        providers_json = tmp_path / "providers.json"

        fail_result = MagicMock(returncode=1)
        ok_result = MagicMock(returncode=0)
        with patch(
            "airflow_breeze.commands.registry_commands.run_command",
            side_effect=[fail_result, ok_result],
        ) as mock_run:
            rc = _run_extract_script(script, "pkg[extra]==1.0", "pkg==1.0", "amazon", providers_json)

        assert rc == 0
        assert mock_run.call_count == 2
        # First call uses extras, second uses base spec
        first_cmd = mock_run.call_args_list[0][0][0]
        second_cmd = mock_run.call_args_list[1][0][0]
        assert "pkg[extra]==1.0" in first_cmd
        assert "pkg==1.0" in second_cmd

    def test_no_fallback_when_specs_are_identical(self, tmp_path):
        script = tmp_path / "extract.py"
        providers_json = tmp_path / "providers.json"

        fail_result = MagicMock(returncode=1)
        with patch(
            "airflow_breeze.commands.registry_commands.run_command",
            return_value=fail_result,
        ) as mock_run:
            rc = _run_extract_script(script, "pkg==1.0", "pkg==1.0", "amazon", providers_json)

        assert rc == 1
        mock_run.assert_called_once()

    def test_returns_fallback_failure_code(self, tmp_path):
        script = tmp_path / "extract.py"
        providers_json = tmp_path / "providers.json"

        fail_result = MagicMock(returncode=1)
        with patch(
            "airflow_breeze.commands.registry_commands.run_command",
            return_value=fail_result,
        ) as mock_run:
            rc = _run_extract_script(script, "pkg[extra]==1.0", "pkg==1.0", "amazon", providers_json)

        assert rc == 1
        assert mock_run.call_count == 2
