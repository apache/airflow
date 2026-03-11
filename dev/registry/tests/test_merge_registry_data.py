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


class TestMerge:
    def test_replaces_existing_provider(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {
                "providers": [
                    {"id": "amazon", "name": "Amazon", "last_updated": "2024-01-01"},
                    {"id": "google", "name": "Google", "last_updated": "2024-02-01"},
                ]
            },
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {
                "modules": [
                    {"id": "amazon-s3-op", "provider_id": "amazon"},
                    {"id": "google-bq-op", "provider_id": "google"},
                ]
            },
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "amazon", "name": "Amazon", "last_updated": "2025-01-01"}]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [{"id": "amazon-s3-op-v2", "provider_id": "amazon"}]},
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
            {"providers": [{"id": "google", "name": "Google", "last_updated": "2024-02-01"}]},
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {"modules": [{"id": "google-bq-op", "provider_id": "google"}]},
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "amazon", "name": "Amazon", "last_updated": "2025-01-01"}]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [{"id": "amazon-s3-op", "provider_id": "amazon"}]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        assert len(result_providers) == 2
        assert {p["id"] for p in result_providers} == {"amazon", "google"}

    def test_providers_sorted_by_name(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": [{"id": "zzz", "name": "Zzz Provider", "last_updated": ""}]},
        )
        existing_modules = _write_json(tmp_path / "existing_modules.json", {"modules": []})
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "aaa", "name": "Aaa Provider", "last_updated": ""}]},
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
                    {"id": "old", "name": "Old Provider", "last_updated": "2020-01-01"},
                ]
            },
        )
        existing_modules = _write_json(
            tmp_path / "existing_modules.json",
            {"modules": [{"id": "old-mod", "provider_id": "old"}]},
        )
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "new", "name": "New Provider", "last_updated": "2025-06-01"}]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [{"id": "new-mod", "provider_id": "new"}]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_modules = json.loads((output_dir / "modules.json").read_text())["modules"]
        # Newest first
        assert result_modules[0]["id"] == "new-mod"
        assert result_modules[1]["id"] == "old-mod"

    def test_missing_existing_modules_file(self, tmp_path, output_dir):
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": [{"id": "google", "name": "Google", "last_updated": ""}]},
        )
        # existing_modules file does not exist
        existing_modules = tmp_path / "nonexistent_modules.json"
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "amazon", "name": "Amazon", "last_updated": ""}]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [{"id": "amazon-s3-op", "provider_id": "amazon"}]},
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
            {"providers": [{"id": "amazon", "name": "Amazon", "last_updated": ""}]},
        )
        new_modules = _write_json(
            tmp_path / "new_modules.json",
            {"modules": [{"id": "amazon-s3-op", "provider_id": "amazon"}]},
        )

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        result_providers = json.loads((output_dir / "providers.json").read_text())["providers"]
        assert len(result_providers) == 1
        assert result_providers[0]["id"] == "amazon"

    def test_output_directory_created_if_missing(self, tmp_path):
        output_dir = tmp_path / "does" / "not" / "exist"
        existing_providers = _write_json(
            tmp_path / "existing_providers.json",
            {"providers": []},
        )
        existing_modules = _write_json(tmp_path / "existing_modules.json", {"modules": []})
        new_providers = _write_json(
            tmp_path / "new_providers.json",
            {"providers": [{"id": "test", "name": "Test", "last_updated": ""}]},
        )
        new_modules = _write_json(tmp_path / "new_modules.json", {"modules": []})

        merge(existing_providers, existing_modules, new_providers, new_modules, output_dir)

        assert (output_dir / "providers.json").exists()
        assert (output_dir / "modules.json").exists()
