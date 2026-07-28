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
"""Unit tests for dev/registry/patch_providers_json.py."""

from __future__ import annotations

import json
import sys
from unittest.mock import patch as mock_patch

import pytest
from patch_providers_json import main, patch


def _write_providers(tmp_path, providers):
    p = tmp_path / "providers.json"
    p.write_text(json.dumps({"providers": providers}, indent=2))
    return p


class TestPatch:
    def test_appends_missing_version(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": ["9.26.0", "9.25.0"]}],
        )
        rc = patch(p, "amazon", ["9.24.0"])
        assert rc == 0
        data = json.loads(p.read_text())
        amazon = next(p for p in data["providers"] if p["id"] == "amazon")
        assert amazon["versions"] == ["9.26.0", "9.25.0", "9.24.0"]

    def test_skips_already_present_version(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": ["9.26.0", "9.25.0"]}],
        )
        before = json.loads(p.read_text())
        rc = patch(p, "amazon", ["9.25.0"])
        after = json.loads(p.read_text())
        assert rc == 0
        # Same content; sorting may rewrite the file but the array is unchanged.
        assert before["providers"][0]["versions"] == after["providers"][0]["versions"]

    def test_unknown_provider_id_errors(self, tmp_path, capsys):
        p = _write_providers(tmp_path, [{"id": "amazon", "version": "9.26.0", "versions": []}])
        rc = patch(p, "nonexistent-provider", ["1.0.0"])
        assert rc == 1
        captured = capsys.readouterr()
        assert "nonexistent-provider" in captured.out

    def test_multiple_versions_one_call(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": ["9.26.0"]}],
        )
        rc = patch(p, "amazon", ["9.24.0", "9.22.0"])
        assert rc == 0
        data = json.loads(p.read_text())
        amazon = next(p for p in data["providers"] if p["id"] == "amazon")
        # Sorted newest-first
        assert amazon["versions"] == ["9.26.0", "9.24.0", "9.22.0"]

    def test_invalid_version_falls_back_to_lex_sort(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [{"id": "weird", "version": "1.0.0", "versions": ["1.0.0", "not-a-semver"]}],
        )
        # Should not raise
        rc = patch(p, "weird", ["custom-tag"])
        assert rc == 0
        data = json.loads(p.read_text())
        weird = next(p for p in data["providers"] if p["id"] == "weird")
        # All three should be present
        assert set(weird["versions"]) == {"1.0.0", "not-a-semver", "custom-tag"}

    def test_includes_latest_when_versions_was_empty(self, tmp_path):
        """Regression: empty `versions[]` must not exclude `provider.version`.

        After patch, providerVersions.js treats non-empty versions[] as
        authoritative. If we patch only the new version into an empty list,
        the latest field would be excluded from the dropdown.
        """
        p = _write_providers(
            tmp_path,
            [{"id": "newish", "version": "1.0.0", "versions": []}],
        )
        rc = patch(p, "newish", ["0.9.0"])
        assert rc == 0
        data = json.loads(p.read_text())
        newish = next(p for p in data["providers"] if p["id"] == "newish")
        assert "1.0.0" in newish["versions"]
        assert "0.9.0" in newish["versions"]

    def test_provider_version_field_unchanged(self, tmp_path):
        """The `version` (singular, latest) field is never touched by the patch."""
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": ["9.26.0", "9.25.0"]}],
        )
        rc = patch(p, "amazon", ["9.24.0"])
        assert rc == 0
        data = json.loads(p.read_text())
        amazon = next(p for p in data["providers"] if p["id"] == "amazon")
        # Latest pointer must remain 9.26.0 even though we added an older version.
        assert amazon["version"] == "9.26.0"

    def test_other_providers_untouched(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [
                {"id": "amazon", "version": "9.26.0", "versions": ["9.26.0"]},
                {"id": "google", "version": "21.2.0", "versions": ["21.2.0", "21.1.0"]},
            ],
        )
        rc = patch(p, "amazon", ["9.24.0"])
        assert rc == 0
        data = json.loads(p.read_text())
        google = next(p for p in data["providers"] if p["id"] == "google")
        assert google["versions"] == ["21.2.0", "21.1.0"]
        assert google["version"] == "21.2.0"


class TestMainCli:
    def test_main_exits_with_patch_returncode(self, tmp_path, capsys):
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": ["9.26.0"]}],
        )
        argv = [
            "patch_providers_json.py",
            "--providers-json",
            str(p),
            "--provider",
            "amazon",
            "--version",
            "9.24.0",
        ]
        with mock_patch.object(sys, "argv", argv):
            rc = main()
        assert rc == 0
        data = json.loads(p.read_text())
        amazon = next(p for p in data["providers"] if p["id"] == "amazon")
        assert "9.24.0" in amazon["versions"]

    def test_main_unknown_provider_exits_1(self, tmp_path):
        p = _write_providers(
            tmp_path,
            [{"id": "amazon", "version": "9.26.0", "versions": []}],
        )
        argv = [
            "patch_providers_json.py",
            "--providers-json",
            str(p),
            "--provider",
            "ghost",
            "--version",
            "1.0.0",
        ]
        with mock_patch.object(sys, "argv", argv):
            rc = main()
        assert rc == 1

    def test_main_requires_version_argument(self, tmp_path):
        p = _write_providers(tmp_path, [])
        argv = [
            "patch_providers_json.py",
            "--providers-json",
            str(p),
            "--provider",
            "amazon",
        ]
        with mock_patch.object(sys, "argv", argv):
            with pytest.raises(SystemExit) as exc:
                main()
            assert exc.value.code != 0
