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
from pathlib import Path
from types import SimpleNamespace

import pytest
import run_generate_constraints as m


def _config(
    latest: Path,
    current: Path,
    constraints_dir: Path | None = None,
    allow_pre_releases: bool = False,
) -> SimpleNamespace:
    return SimpleNamespace(
        latest_constraints_file=latest,
        current_constraints_file=current,
        constraints_dir=constraints_dir if constraints_dir is not None else latest.parent,
        python="3.10",
        allow_pre_releases=allow_pre_releases,
    )


def _file(*, yanked: bool = False, requires_python: str | None = ">=3.10") -> dict:
    return {"yanked": yanked, "requires_python": requires_python}


class _FakeResponse:
    def __init__(self, payload: dict, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise AssertionError(f"Unexpected status {self.status_code}")


class TestReadProviderVersionsFromConstraints:
    def test_extracts_only_providers_and_strips_markers(self, tmp_path):
        constraints = tmp_path / "constraints.txt"
        constraints.write_text(
            "requests==2.0.0\n"
            "apache-airflow-providers-amazon==8.0.0\n"
            "apache-airflow-providers-http==4.0.0; python_version < '3.11'\n"
            "  apache-airflow-providers-google==10.0.0  \n"
            "apache-airflow-providers-broken-no-version\n"
        )
        assert m._read_provider_versions_from_constraints(constraints) == {
            "apache-airflow-providers-amazon": "8.0.0",
            "apache-airflow-providers-http": "4.0.0",
            "apache-airflow-providers-google": "10.0.0",
        }


class TestCheckProvidersNotDowngraded:
    def test_exits_and_writes_slack_payload_when_a_provider_is_downgraded(self, tmp_path, monkeypatch):
        monkeypatch.delenv("SLACK_CHANNEL", raising=False)
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "apache/airflow")
        monkeypatch.setenv("GITHUB_RUN_ID", "12345")
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\napache-airflow-providers-google==10.0.0\n")
        # amazon is downgraded, google is upgraded.
        current.write_text(
            "apache-airflow-providers-amazon==7.5.0\napache-airflow-providers-google==10.1.0\n"
        )
        with pytest.raises(SystemExit) as exc_info:
            m.check_providers_not_downgraded(_config(latest, current, constraints_dir=tmp_path))
        assert exc_info.value.code == 1
        payload = json.loads((tmp_path / "provider-downgrade-slack-message.json").read_text())
        assert payload["channel"] == "internal-airflow-ci-cd"
        text_blocks = json.dumps(payload["blocks"])
        assert "apache-airflow-providers-amazon" in text_blocks
        assert "apache-airflow-providers-google" not in text_blocks
        assert "https://github.com/apache/airflow/actions/runs/12345" in text_blocks

    def test_slack_channel_is_overridable_via_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("SLACK_CHANNEL", "some-other-channel")
        m.write_provider_downgrade_slack_message(
            _config(tmp_path / "latest.txt", tmp_path / "current.txt", constraints_dir=tmp_path),
            [("apache-airflow-providers-amazon", "8.0.0", "7.5.0")],
        )
        payload = json.loads((tmp_path / "provider-downgrade-slack-message.json").read_text())
        assert payload["channel"] == "some-other-channel"

    def test_skips_the_check_for_a_release_candidate(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\n")
        current.write_text("apache-airflow-providers-amazon==7.5.0\n")

        m.check_providers_not_downgraded(_config(latest, current, allow_pre_releases=True))

        assert not (tmp_path / "provider-downgrade-slack-message.json").exists()

    def test_passes_when_no_provider_is_downgraded(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\n")
        current.write_text(
            "apache-airflow-providers-amazon==8.1.0\napache-airflow-providers-google==10.0.0\n"
        )
        m.check_providers_not_downgraded(_config(latest, current))

    def test_passes_when_provider_removed_from_current(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==8.0.0\n")
        current.write_text("apache-airflow-providers-google==10.0.0\n")
        m.check_providers_not_downgraded(_config(latest, current))

    def test_skips_when_latest_file_missing(self, tmp_path):
        current = tmp_path / "current.txt"
        current.write_text("apache-airflow-providers-amazon==7.0.0\n")
        m.check_providers_not_downgraded(_config(tmp_path / "missing.txt", current))

    def test_skips_uncomparable_versions_without_error(self, tmp_path):
        latest = tmp_path / "latest.txt"
        current = tmp_path / "current.txt"
        latest.write_text("apache-airflow-providers-amazon==not-a-version\n")
        current.write_text("apache-airflow-providers-amazon==also-bad\n")
        m.check_providers_not_downgraded(_config(latest, current))


class TestFindNewestVersionInPypi:
    @pytest.mark.parametrize(
        "releases, allow_pre_releases, expected",
        [
            pytest.param(
                {"1.0.0": [_file()], "1.1.0": [_file()]},
                False,
                "1.1.0",
                id="newest-final-release",
            ),
            pytest.param(
                {"1.1.0": [_file()], "1.2.0rc1": [_file()]},
                True,
                "1.2.0rc1",
                id="candidate-newer-than-the-release-wins-when-allowed",
            ),
            pytest.param(
                {"1.1.0": [_file()], "1.2.0rc1": [_file()]},
                False,
                "1.1.0",
                id="candidate-is-ignored-when-not-allowed",
            ),
            pytest.param(
                {"1.1.0rc1": [_file()], "1.1.0": [_file()]},
                True,
                "1.1.0",
                id="superseded-candidate-loses-to-its-release",
            ),
            pytest.param(
                {"1.0.0": [_file()], "1.1.0": [_file(yanked=True)]},
                False,
                "1.0.0",
                id="yanked-version-is-passed-over",
            ),
            pytest.param(
                {"1.0.0": [_file()], "1.1.0": []},
                False,
                "1.0.0",
                id="version-with-no-files-is-passed-over",
            ),
            pytest.param(
                {"1.0.0": [_file()], "1.1.0": [_file(requires_python=">=3.12")]},
                False,
                "1.0.0",
                id="version-excluded-by-requires-python-is-passed-over",
            ),
            pytest.param(
                {"1.0.0": [_file()], "not-a-version": [_file()]},
                False,
                "1.0.0",
                id="unparseable-version-is-passed-over",
            ),
            pytest.param(
                {"1.0.0": [_file(requires_python=None)]},
                False,
                "1.0.0",
                id="file-without-requires-python-is-installable",
            ),
            pytest.param(
                {"1.0.0": [_file(requires_python="not-a-specifier")]},
                False,
                "1.0.0",
                id="file-with-broken-requires-python-is-not-excluded",
            ),
            pytest.param({"1.0.0": [_file(yanked=True)]}, False, None, id="nothing-installable-is-unpinned"),
            pytest.param({"1.0.0rc1": [_file()]}, False, None, id="only-a-candidate-is-unpinned"),
        ],
    )
    def test_picks_the_newest_installable_version(self, monkeypatch, releases, allow_pre_releases, expected):
        monkeypatch.setattr(m.requests, "get", lambda url, timeout: _FakeResponse({"releases": releases}))

        newest = m.find_newest_version_in_pypi("apache-airflow-providers-amazon", "3.10", allow_pre_releases)

        assert newest == expected

    def test_unpublished_distribution_is_unpinned(self, monkeypatch):
        monkeypatch.setattr(m.requests, "get", lambda url, timeout: _FakeResponse({}, status_code=404))

        assert m.find_newest_version_in_pypi("apache-airflow-providers-brand-new", "3.10", True) is None

    def test_retries_when_pypi_resets_the_connection(self, monkeypatch):
        calls = {"n": 0}

        def flaky_get(url, timeout):
            calls["n"] += 1
            if calls["n"] == 1:
                raise m.requests.ConnectionError("Connection reset by peer")
            return _FakeResponse({"releases": {"1.2.0": [_file()]}})

        monkeypatch.setattr(m.requests, "get", flaky_get)
        monkeypatch.setattr(m.time, "sleep", lambda seconds: None)

        newest = m.find_newest_version_in_pypi("apache-airflow-providers-amazon", "3.10", False)

        assert newest == "1.2.0"
        assert calls["n"] == 2

    def test_connection_reset_is_raised_after_retries_are_exhausted(self, monkeypatch):
        calls = {"n": 0}

        def always_reset(url, timeout):
            calls["n"] += 1
            raise m.requests.ConnectionError("Connection reset by peer")

        monkeypatch.setattr(m.requests, "get", always_reset)
        monkeypatch.setattr(m.time, "sleep", lambda seconds: None)

        with pytest.raises(m.requests.ConnectionError, match="Connection reset by peer"):
            m.find_newest_version_in_pypi("apache-airflow-providers-amazon", "3.10", False)

        assert calls["n"] == m.PYPI_LOOKUP_ATTEMPTS


class TestBuildPinnedProviderRequirements:
    """Providers are pinned at what PyPI holds rather than left for the resolution to pick."""

    def test_every_provider_is_pinned_to_its_newest_pypi_version(self, monkeypatch):
        monkeypatch.setattr(
            m,
            "get_all_active_provider_distributions",
            lambda python_version=None: [
                "apache-airflow-providers-amazon",
                "apache-airflow-providers-cncf-kubernetes",
                "apache-airflow-providers-brand-new",
            ],
        )
        newest = {
            "apache-airflow-providers-amazon": "9.0.0rc1",
            "apache-airflow-providers-cncf-kubernetes": "10.20.0",
            "apache-airflow-providers-brand-new": None,
        }
        monkeypatch.setattr(
            m, "find_newest_version_in_pypi", lambda dist, python_version, allow_pre_releases: newest[dist]
        )

        assert m.build_pinned_provider_requirements("3.10", True) == [
            "apache-airflow-providers-amazon==9.0.0rc1",
            "apache-airflow-providers-cncf-kubernetes==10.20.0",
        ]

    def test_the_python_version_and_the_pre_release_choice_reach_the_lookup(self, monkeypatch):
        """Providers excluded on a Python version must not be named, nor pinned to a version it cannot install."""
        seen_for_distributions: list[str | None] = []
        seen_for_lookup: list[tuple[str, bool]] = []

        def fake_distributions(python_version=None):
            seen_for_distributions.append(python_version)
            return ["apache-airflow-providers-amazon"]

        def fake_lookup(distribution, python_version, allow_pre_releases):
            seen_for_lookup.append((python_version, allow_pre_releases))
            return "9.0.0"

        monkeypatch.setattr(m, "get_all_active_provider_distributions", fake_distributions)
        monkeypatch.setattr(m, "find_newest_version_in_pypi", fake_lookup)

        m.build_pinned_provider_requirements("3.14", False)

        assert seen_for_distributions == ["3.14"]
        assert seen_for_lookup == [("3.14", False)]
