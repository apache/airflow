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
"""Unit tests for dev/registry/merge_registry_data.py."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from merge_registry_data import merge


@pytest.fixture
def output_dir(tmp_path):
    out = tmp_path / "output"
    out.mkdir()
    return out


def _write_json(path: Path, data: dict) -> Path:
    path.write_text(json.dumps(data, indent=2))
    return path


def _provider(provider_id: str, name: str, last_updated: str) -> dict:
    return {
        "id": provider_id,
        "name": name,
        "package_name": f"apache-airflow-providers-{provider_id}",
        "description": f"{name} provider",
        "lifecycle": "production",
        "logo": None,
        "version": "1.0.0",
        "versions": ["1.0.0"],
        "airflow_versions": ["3.0+"],
        "pypi_downloads": {"weekly": 0, "monthly": 0, "total": 0},
        "module_counts": {"operator": 1},
        "categories": [],
        "connection_types": [],
        "requires_python": ">=3.10",
        "dependencies": [],
        "optional_extras": {},
        "dependents": [],
        "related_providers": [],
        "docs_url": "https://example.invalid/docs",
        "source_url": "https://example.invalid/source",
        "pypi_url": "https://example.invalid/pypi",
        "first_released": "",
        "last_updated": last_updated,
    }


def _module(module_id: str, provider_id: str) -> dict:
    return {
        "id": module_id,
        "name": "ExampleOperator",
        "type": "operator",
        "import_path": f"airflow.providers.{provider_id}.operators.example.ExampleOperator",
        "module_path": f"airflow.providers.{provider_id}.operators.example",
        "short_description": "Example operator",
        "docs_url": "https://example.invalid/docs",
        "source_url": "https://example.invalid/source",
        "category": "test",
        "provider_id": provider_id,
        "provider_name": provider_id.capitalize(),
    }


class TestMerge:
    def test_replaces_existing_provider(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {
                "providers": [
                    _provider("amazon", "Amazon", "2024-01-01"),
                    _provider("google", "Google", "2024-02-01"),
                ]
            },
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {
                "modules": [
                    _module("amazon-s3-op", "amazon"),
                    _module("google-bq-op", "google"),
                ]
            },
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("amazon", "Amazon", "2025-01-01")]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [_module("amazon-s3-op-v2", "amazon")]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        result_modules = json.loads((output_dir / "modules.json").read_text())["modules"]

        # Both providers present
        provider_ids = {p["id"] for p in result_providers}
        assert provider_ids == {"amazon", "google"}

        # Amazon was replaced (new last_updated)
        amazon = next(p for p in result_providers if p["id"] == "amazon")
        assert amazon["last_updated"] == "2025-01-01"

        # Old amazon module removed, new one added, google kept
        module_ids = {m["id"] for m in result_modules}
        assert "amazon-s3-op" not in module_ids
        assert "amazon-s3-op-v2" in module_ids
        assert "google-bq-op" in module_ids

    def test_adds_new_provider(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": [_provider("google", "Google", "2024-02-01")]},
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {"modules": [_module("google-bq-op", "google")]},
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("amazon", "Amazon", "2025-01-01")]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [_module("amazon-s3-op", "amazon")]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        assert len(result_providers) == 2
        assert {p["id"] for p in result_providers} == {"amazon", "google"}

    def test_providers_sorted_by_name(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": [_provider("zzz", "Zzz Provider", "")]},
        )
        existing_modules = _write_json(tmp_path / "existing_modules.json", {"modules": []})
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("aaa", "Aaa Provider", "")]},
        )
        new_modules = _write_json(tmp_path / "new_modules.json", {"modules": []})

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        names = [p["name"] for p in result_providers]
        assert names == ["Aaa Provider", "Zzz Provider"]

    def test_modules_sorted_by_provider_last_updated(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {
                "providers": [
                    _provider("old", "Old Provider", "2020-01-01"),
                ]
            },
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {"modules": [_module("old-mod", "old")]},
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("new", "New Provider", "2025-06-01")]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [_module("new-mod", "new")]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_modules = json.loads((output_dir / "modules.json").read_text())["modules"]
        # Newest first
        assert result_modules[0]["id"] == "new-mod"
        assert result_modules[1]["id"] == "old-mod"

    def test_missing_existing_modules_file(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": [_provider("google", "Google", "")]},
        )
        # existing_modules file does not exist
        existing_modules = tmp_path / "nonexistent_modules.json"
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("amazon", "Amazon", "")]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [_module("amazon-s3-op", "amazon")]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_modules = json.loads((output_dir / "modules.json").read_text())["modules"]
        assert len(result_modules) == 1
        assert result_modules[0]["id"] == "amazon-s3-op"

    def test_missing_existing_providers_file(self, tmp_path, output_dir):
        # existing_providers file does not exist
        existing_providers = tmp_path / "nonexistent_providers.json"
        existing_modules = _write_json(tmp_path / "existing_modules.json", {"modules": []})
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("amazon", "Amazon", "")]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [_module("amazon-s3-op", "amazon")]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        assert len(result_providers) == 1
        assert result_providers[0]["id"] == "amazon"

    def test_missing_new_modules_file(self, tmp_path, output_dir):
        """Incremental extract with --provider skips modules.json; merge should keep existing modules."""
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {
                "providers": [
                    _provider("amazon", "Amazon", "2024-01-01"),
                    _provider("google", "Google", "2024-02-01"),
                ]
            },
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {
                "modules": [
                    _module("amazon-s3-op", "amazon"),
                    _module("google-bq-op", "google"),
                ]
            },
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("amazon", "Amazon", "2025-01-01")]},
        )
        # new_modules file does not exist (--provider mode skips modules.json)
        new_modules = tmp_path / "nonexistent_modules.json"

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_modules = json.loads((output_dir / "modules.json").read_text())["modules"]
        # Existing modules for non-updated providers are kept
        assert any(m["id"] == "google-bq-op" for m in result_modules)
        # Existing modules for the updated provider are removed (no new ones to replace them)
        assert not any(m["provider_id"] == "amazon" for m in result_modules)

    def test_output_directory_created_if_missing(self, tmp_path):
        output_dir = tmp_path / "does" / "not" / "exist"
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": []},
        )
        existing_modules = _write_json(tmp_path / "existing_modules.json", {"modules": []})
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [_provider("test", "Test", "")]},
        )
        new_modules = _write_json(tmp_path / "new_modules.json", {"modules": []})

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        assert (output_dir / "providers.json").exists()
        assert (output_dir / "modules.json").exists()
