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

import http.client
import io
import json
import textwrap
import urllib.error
from unittest import mock

import lower_cross_provider_dependencies as m
import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

COMPAT = "apache-airflow-providers-common-compat"
SQL = "apache-airflow-providers-common-sql"


def _pyproject(*dependencies: str, name: str = "apache-airflow-providers-example") -> str:
    lines = "\n".join(f"    {dependency}" for dependency in dependencies)
    return textwrap.dedent(
        """\
        [project]
        name = "{name}"
        dependencies = [
        {lines}
        ]
        """
    ).format(name=name, lines=lines)


def _file(yanked: bool = False) -> dict:
    return {"yanked": yanked}


RELEASES = {
    COMPAT: {
        "1.7.0": [_file()],
        "1.8.0": [_file()],
        "1.9.0": [_file(yanked=True)],
        "1.10.0": [_file()],
        "1.19.0rc1": [_file()],
    },
    SQL: {"1.20.0": [_file()], "1.30.0": [_file()]},
}


class TestGetCrossProviderRequirements:
    def test_only_provider_dependencies_are_returned(self):
        text = _pyproject(
            '"apache-airflow>=2.11.0",',
            f'"{COMPAT}>=1.8.0",',
            '"requests>=2.0",',
        )
        result = m.get_cross_provider_requirements(text)
        assert [(str(r), next_version) for r, next_version in result] == [(f"{COMPAT}>=1.8.0", False)]

    def test_detects_use_next_version_comment(self):
        text = _pyproject(f'"{COMPAT}>=1.8.0",  # use next version', f'"{SQL}>=1.20.0",')
        result = {r.name: next_version for r, next_version in m.get_cross_provider_requirements(text)}
        assert result == {COMPAT: True, SQL: False}

    def test_skips_requirements_whose_marker_does_not_apply(self):
        text = _pyproject(f"\"{COMPAT}>=1.8.0; python_version < '3.0'\",")
        assert m.get_cross_provider_requirements(text) == []


class TestLowestFinalRelease:
    def test_skips_yanked_and_prereleases(self):
        assert m.lowest_final_release(SpecifierSet(">=1.9.0"), RELEASES[COMPAT]) == Version("1.10.0")

    def test_none_when_only_a_release_candidate_matches(self):
        assert m.lowest_final_release(SpecifierSet(">=1.19.0"), RELEASES[COMPAT]) is None

    def test_skips_releases_without_files(self):
        assert m.lowest_final_release(SpecifierSet(">=1.0"), {"1.0.0": [], "2.0.0": [_file()]}) == Version(
            "2.0.0"
        )


class TestCombinedSpecifier:
    def test_intersects_base_requirements_and_ignores_extras(self):
        installed = {
            "apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"],
            "apache-airflow-providers-openlineage": [f"{COMPAT}>=1.10.0"],
            "apache-airflow-providers-other": [f'{COMPAT}>=5.0.0; extra == "something"'],
        }
        specifier, requirers, unreleased_requirer = m.combined_specifier(COMPAT, installed)
        assert specifier.contains("1.10.0")
        assert not specifier.contains("1.8.0")
        assert specifier.contains("6.0.0")
        assert requirers == [
            "apache-airflow-providers-example (>=1.8.0)",
            "apache-airflow-providers-openlineage (>=1.10.0)",
        ]
        assert unreleased_requirer is None

    def test_skips_unparsable_requirements(self, capsys):
        installed = {
            "azure-kusto-data": ["azure-core (>=1.11.0<2)"],
            "apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"],
            "broken-provider": [f"{COMPAT} (>=1.9.0<2)"],
        }
        specifier, requirers, unreleased_requirer = m.combined_specifier(COMPAT, installed)
        assert specifier == SpecifierSet(">=1.8.0")
        assert requirers == ["apache-airflow-providers-example (>=1.8.0)"]
        assert unreleased_requirer is None
        # Only the entry about the provider being lowered is worth a log line.
        assert capsys.readouterr().err.splitlines() == [
            f"Ignoring unparsable requirement '{COMPAT} (>=1.9.0<2)' of broken-provider"
        ]

    def test_excludes_requirer_that_needs_an_unreleased_release(self):
        # "fab" declares a numeric floor for logging/tooling purposes, but its own
        # "# use next version" marker means that floor is not actually sufficient: only the
        # unreleased workspace version has what it needs. Its bound must not count towards the
        # intersection, and its name must come back so the caller keeps the workspace version too.
        installed = {
            "apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"],
            "apache-airflow-providers-fab": [f"{COMPAT}>=1.18.0"],
        }
        fab_pyproject = _pyproject(f'"{COMPAT}>=1.18.0",  # use next version')
        specifier, requirers, unreleased_requirer = m.combined_specifier(
            COMPAT, installed, {"apache-airflow-providers-fab": fab_pyproject}
        )
        assert specifier == SpecifierSet(">=1.8.0")
        assert requirers == ["apache-airflow-providers-example (>=1.8.0)"]
        assert unreleased_requirer == "apache-airflow-providers-fab"


class TestDecide:
    @staticmethod
    def _fetch(name):
        return RELEASES[name]

    def test_pins_declared_lower_bound(self):
        text = _pyproject(f'"{COMPAT}>=1.8.0",')
        installed = {"apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"]}
        assert [d.pin for d in m.decide(text, installed, self._fetch)] == [f"{COMPAT}==1.8.0"]

    def test_respects_higher_lower_bound_of_other_installed_provider(self):
        text = _pyproject(f'"{COMPAT}>=1.8.0",')
        installed = {
            "apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"],
            "apache-airflow-providers-openlineage": [f"{COMPAT}>=1.9.0"],
        }
        (decision,) = m.decide(text, installed, self._fetch)
        assert decision.pin == f"{COMPAT}==1.10.0"
        assert "apache-airflow-providers-openlineage (>=1.9.0)" in decision.reason

    @pytest.mark.parametrize(
        ("dependency", "reason"),
        [
            pytest.param(f'"{COMPAT}>=1.8.0",  # use next version', "use next version", id="next-version"),
            pytest.param(f'"{COMPAT}",', "no lower bound", id="no-lower-bound"),
            pytest.param(f'"{COMPAT}>=1.19.0",', "no final release", id="only-release-candidate"),
        ],
    )
    def test_keeps_workspace_version(self, dependency, reason):
        text = _pyproject(dependency)
        installed = {"apache-airflow-providers-example": [dependency.split('"')[1]]}
        (decision,) = m.decide(text, installed, self._fetch)
        assert decision.pin is None
        assert reason in decision.reason

    def test_keeps_workspace_version_when_another_installed_provider_needs_unreleased_release(self):
        # Regression test: lowering a provider (e.g. "google") that only requires an old release
        # of a dependency (e.g. "common-compat") must not pin it to that old release when another
        # installed provider (e.g. "fab", pulled in as an extra) needs unreleased content from
        # that same dependency. Its own dependency line has no "# use next version" marker, so
        # the marker on fab's line is only visible via provider_pyproject_texts.
        text = _pyproject(f'"{COMPAT}>=1.8.0",')
        installed = {
            "apache-airflow-providers-example": [f"{COMPAT}>=1.8.0"],
            "apache-airflow-providers-fab": [f"{COMPAT}>=1.18.0"],
        }
        fab_pyproject = _pyproject(f'"{COMPAT}>=1.18.0",  # use next version')
        (decision,) = m.decide(text, installed, self._fetch, {"apache-airflow-providers-fab": fab_pyproject})
        assert decision.pin is None
        assert "apache-airflow-providers-fab" in decision.reason
        assert "unreleased" in decision.reason

    def test_fetches_releases_only_for_pinnable_dependencies(self):
        text = _pyproject(f'"{COMPAT}>=1.8.0",  # use next version', f'"{SQL}>=1.20.0",')
        fetched = []

        def fetch(name):
            fetched.append(name)
            return RELEASES[name]

        decisions = m.decide(text, {"apache-airflow-providers-example": [f"{SQL}>=1.20.0"]}, fetch)
        assert fetched == [SQL]
        assert [d.pin for d in decisions] == [None, f"{SQL}==1.20.0"]


class TestDecideUntilStable:
    RELEASES = {
        COMPAT: {"1.8.0": [_file()], "1.10.0": [_file()], "1.17.0": [_file()]},
        SQL: {"1.32.0": [_file()], "2.1.0": [_file()]},
    }

    def _fetch(self, name):
        return self.RELEASES[name]

    def test_uses_requirements_of_the_pinned_release_not_the_workspace_one(self):
        # The workspace common.sql needs common.compat>=1.17.0, the release it is lowered to
        # only >=1.8.0, so common.compat can go all the way down to the tested provider's bound.
        text = _pyproject(f'"{SQL}>=1.32.0",', f'"{COMPAT}>=1.8.0",')
        installed = {
            "apache-airflow-providers-example": [f"{SQL}>=1.32.0", f"{COMPAT}>=1.8.0"],
            SQL: [f"{COMPAT}>=1.17.0"],
        }
        requires = {(SQL, "1.32.0"): [f"{COMPAT}>=1.8.0"]}
        decisions = m.decide_until_stable(text, installed, self._fetch, lambda n, v: requires.get((n, v), []))
        assert [d.pin for d in decisions] == [f"{SQL}==1.32.0", f"{COMPAT}==1.8.0"]

    def test_pinned_release_can_raise_the_floor(self):
        text = _pyproject(f'"{SQL}>=1.32.0",', f'"{COMPAT}>=1.8.0",')
        installed = {
            "apache-airflow-providers-example": [f"{SQL}>=1.32.0", f"{COMPAT}>=1.8.0"],
            SQL: [f"{COMPAT}>=1.8.0"],
        }
        requires = {(SQL, "1.32.0"): [f"{COMPAT}>=1.10.0"]}
        decisions = m.decide_until_stable(text, installed, self._fetch, lambda n, v: requires.get((n, v), []))
        assert [d.pin for d in decisions] == [f"{SQL}==1.32.0", f"{COMPAT}==1.10.0"]

    def test_no_pins_needs_no_metadata(self):
        text = _pyproject(f'"{COMPAT}>=1.8.0",  # use next version')

        def fail(name, version):
            raise AssertionError("requires_dist should not be fetched")

        decisions = m.decide_until_stable(text, {}, self._fetch, fail)
        assert [d.pin for d in decisions] == [None]

    def test_raises_when_pins_keep_changing(self, monkeypatch):
        flip = iter(
            [[m.Decision(COMPAT, f"{COMPAT}==1.8.0", "")], [m.Decision(COMPAT, f"{COMPAT}==1.10.0", "")]] * 10
        )
        monkeypatch.setattr(m, "decide", lambda *args: next(flip))
        with pytest.raises(RuntimeError, match="did not settle"):
            m.decide_until_stable("", {}, self._fetch, lambda n, v: [])


class TestFindProviderPyprojectTexts:
    def test_maps_declared_project_name_to_its_own_pyproject_text(self, tmp_path, monkeypatch):
        monkeypatch.setattr(m, "AIRFLOW_ROOT_PATH", tmp_path)
        fab_dir = tmp_path / "providers" / "fab"
        fab_dir.mkdir(parents=True)
        fab_text = _pyproject(f'"{COMPAT}>=1.18.0",  # use next version', name="apache-airflow-providers-fab")
        (fab_dir / "pyproject.toml").write_text(fab_text)

        result = m.find_provider_pyproject_texts()

        assert result == {"apache-airflow-providers-fab": fab_text}

    def test_skips_unparsable_pyproject_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(m, "AIRFLOW_ROOT_PATH", tmp_path)
        broken_dir = tmp_path / "providers" / "broken"
        broken_dir.mkdir(parents=True)
        (broken_dir / "pyproject.toml").write_text("not [ valid toml")

        assert m.find_provider_pyproject_texts() == {}


class TestFetchJson:
    URL = "https://pypi.org/pypi/apache-airflow-providers-example/json"

    @staticmethod
    def _response(payload: dict) -> mock.MagicMock:
        response = mock.MagicMock()
        response.__enter__.return_value = io.BytesIO(json.dumps(payload).encode())
        return response

    @staticmethod
    def _http_error(code: int) -> urllib.error.HTTPError:
        return urllib.error.HTTPError(TestFetchJson.URL, code, "error", hdrs=None, fp=None)  # type: ignore[arg-type]

    @pytest.mark.parametrize(
        "error",
        [
            pytest.param(TimeoutError("The read operation timed out"), id="timeout"),
            pytest.param(http.client.IncompleteRead(b"{"), id="truncated-response"),
        ],
    )
    def test_retries_transient_errors(self, error):
        sleeps: list[float] = []
        responses = [error, self._response({"releases": {}})]
        with mock.patch.object(m.urllib.request, "urlopen", side_effect=responses):
            assert m.fetch_json(self.URL, sleep=sleeps.append) == {"releases": {}}
        assert sleeps == [m.PYPI_RETRY_DELAY_SECONDS]

    def test_missing_page_is_not_retried(self):
        with mock.patch.object(m.urllib.request, "urlopen", side_effect=self._http_error(404)) as urlopen:
            assert m.fetch_json(self.URL, sleep=pytest.fail) is None
            assert m.fetch_pypi_releases("apache-airflow-providers-example") == {}
        assert urlopen.call_count == 2

    def test_raises_after_last_attempt(self):
        sleeps: list[float] = []
        with mock.patch.object(m.urllib.request, "urlopen", side_effect=self._http_error(503)):
            with pytest.raises(RuntimeError, match=f"after {m.PYPI_ATTEMPTS} attempts"):
                m.fetch_json(self.URL, sleep=sleeps.append)
        assert len(sleeps) == m.PYPI_ATTEMPTS - 1
