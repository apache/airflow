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

import subprocess
import textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest
import requests
from common_prek_utils import AIRFLOW_ROOT_PATH
from packaging.requirements import Requirement
from packaging.version import Version
from upgrade_dependency_floors import (
    RESOLVE_COMMANDS,
    Bump,
    Edit,
    FloorConfig,
    LockAlreadyBrokenError,
    Report,
    RequirementSite,
    ResolveResult,
    apply_bump,
    apply_with_rollback,
    build_bump,
    find_group_target,
    find_requirements,
    find_target_version,
    get_exclusion_reason,
    get_workspace_pyprojects,
    is_curated,
    load_config,
    main,
    parse_duration,
    render_report,
    resolve_check,
    revert_bump,
    rewrite_requirement,
    run,
)

CONFIG = """
[tool.airflow.dependency-floors]
min-age = "180 days"
packages = ["boto3", "botocore", "Google_Cloud-*"]
groups = [["boto3", "botocore"]]

[tool.airflow.dependency-floors.exclude]
Sagemaker_Studio = "Ask AWS first"
"""


def _write(tmp_path, content):
    path = tmp_path / "pyproject.toml"
    path.write_text(textwrap.dedent(content))
    return path


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param("180 days", timedelta(days=180), id="days"),
        pytest.param("1 day", timedelta(days=1), id="singular"),
        pytest.param("12 hours", timedelta(hours=12), id="hours"),
        pytest.param("30 minutes", timedelta(minutes=30), id="minutes"),
        pytest.param(" 1.5 days ", timedelta(days=1.5), id="fraction-and-spaces"),
    ],
)
def test_parse_duration(value, expected):
    assert parse_duration(value) == expected


@pytest.mark.parametrize("value", ["", "6 months", "days", "-1 days"])
def test_parse_duration_rejects(value):
    with pytest.raises(ValueError, match="duration"):
        parse_duration(value)


def test_load_config_canonicalizes_patterns(tmp_path):
    config = load_config(_write(tmp_path, CONFIG))
    assert config == FloorConfig(
        min_age=timedelta(days=180),
        packages=("boto3", "botocore", "google-cloud-*"),
        groups=(("boto3", "botocore"),),
        exclude={"sagemaker-studio": "Ask AWS first"},
    )


def test_load_config_missing_section(tmp_path):
    with pytest.raises(ValueError, match=r"\[tool.airflow.dependency-floors\]"):
        load_config(_write(tmp_path, "[project]\nname = 'x'\n"))


def test_load_config_group_member_not_curated(tmp_path):
    content = CONFIG.replace('groups = [["boto3", "botocore"]]', 'groups = [["boto3", "aiobotocore"]]')
    with pytest.raises(ValueError, match="aiobotocore"):
        load_config(_write(tmp_path, content))


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        pytest.param("boto3", True, id="exact"),
        pytest.param("google-cloud-storage", True, id="glob"),
        pytest.param("google_cloud_storage", True, id="non-canonical"),
        pytest.param("google-api-core", False, id="no-match"),
    ],
)
def test_is_curated(tmp_path, name, expected):
    assert is_curated(name, load_config(_write(tmp_path, CONFIG))) is expected


def test_repository_config_loads():
    config = load_config(AIRFLOW_ROOT_PATH / "pyproject.toml")
    assert config.min_age == timedelta(days=180)
    assert ("boto3", "botocore") in config.groups


PROVIDER = """
[project]
name = "apache-airflow-providers-amazon"
dependencies = [
    "Boto3>=1.41.0",
    "apache-airflow-core>=3.0.0",
    "foo @ https://example.com/foo.whl",
]
[project.optional-dependencies]
"s3fs" = ["s3fs>=2023.10.0"]
[dependency-groups]
dev = ["boto3>=1.41.0", {include-group = "docs"}]
[build-system]
requires = ["hatchling==1.31.0"]
"""


def test_find_requirements_canonicalizes_names(tmp_path):
    path = _write(tmp_path, PROVIDER)
    sites = find_requirements([path], frozenset({"apache-airflow-core"}))
    assert [(s.section, s.raw) for s in sites["boto3"]] == [
        ("project.dependencies", "Boto3>=1.41.0"),
        ('dependency-groups."dev"', "boto3>=1.41.0"),
    ]


def test_find_requirements_skips_workspace_and_urls(tmp_path):
    sites = find_requirements([_write(tmp_path, PROVIDER)], frozenset({"apache-airflow-core"}))
    assert "apache-airflow-core" not in sites
    assert "foo" not in sites
    # build-system.requires is never edited
    assert "hatchling" not in sites


def _site(raw: str, path: str = "p/pyproject.toml") -> RequirementSite:
    return RequirementSite(
        path=Path(path), section="project.dependencies", raw=raw, requirement=Requirement(raw)
    )


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param("boto3>=1.41,<2", id="upper"),
        pytest.param("boto3>=1.41,<=1.50", id="upper-inclusive"),
        pytest.param("boto3>=1.41,!=1.42.0", id="exclusion"),
        pytest.param("boto3==1.41.0", id="pin"),
        pytest.param("boto3~=1.41", id="compatible"),
        pytest.param("boto3===1.41.0", id="arbitrary"),
    ],
)
def test_exclusion_when_held_back(tmp_path, raw):
    config = load_config(_write(tmp_path, CONFIG))
    reason = get_exclusion_reason("boto3", [_site("boto3>=1.40"), _site(raw, "q/pyproject.toml")], config)
    assert reason is not None
    assert "q/pyproject.toml" in reason


def test_exclusion_explicit_list(tmp_path):
    config = load_config(_write(tmp_path, CONFIG))
    assert (
        get_exclusion_reason("sagemaker-studio", [_site("sagemaker-studio>=1.0")], config) == "Ask AWS first"
    )


def test_no_exclusion_for_plain_floor(tmp_path):
    config = load_config(_write(tmp_path, CONFIG))
    assert get_exclusion_reason("boto3", [_site("boto3>=1.40; python_version < '3.14'")], config) is None


NOW = datetime(2026, 9, 24, tzinfo=timezone.utc)
AGE = timedelta(days=180)


def _files(*uploads: str, yanked: bool = False) -> list[dict]:
    return [{"upload_time_iso_8601": u, "yanked": yanked} for u in uploads]


RELEASES = {
    "1.0.0": _files("2025-01-01T00:00:00.000000Z"),
    # the earliest upload of a release counts
    "1.1.0": _files("2026-03-01T00:00:00.000000Z", "2026-06-01T00:00:00.000000Z"),
    "1.2.0": _files("2026-03-10T00:00:00.000000Z", yanked=True),
    "1.3.0rc1": _files("2026-01-01T00:00:00.000000Z"),
    "1.3.0.dev1": _files("2026-01-01T00:00:00.000000Z"),
    "1.4.0": _files("2026-09-01T00:00:00.000000Z"),
    "1.5.0": [],
}


def test_target_is_newest_old_enough_final_release():
    assert find_target_version(RELEASES, AGE, NOW) == Version("1.1.0")


@pytest.mark.parametrize(
    ("upload", "eligible"),
    [
        pytest.param("2026-03-28T00:00:00.000000Z", True, id="exactly-min-age"),
        pytest.param("2026-03-28T00:00:01.000000Z", False, id="one-second-too-new"),
    ],
)
def test_min_age_boundary(upload, eligible):
    target = find_target_version({"2.0.0": _files(upload)}, AGE, NOW)
    assert (target == Version("2.0.0")) is eligible


def test_no_release_old_enough():
    assert find_target_version({"9.0.0": _files("2026-09-20T00:00:00.000000Z")}, AGE, NOW) is None


def test_group_target_is_common_version():
    boto3 = {"1.40.0": _files("2026-01-01T00:00:00Z"), "1.40.5": _files("2026-02-01T00:00:00Z")}
    botocore = {"1.40.0": _files("2026-01-01T00:00:00Z"), "1.40.3": _files("2026-02-01T00:00:00Z")}
    assert find_group_target({"boto3": boto3, "botocore": botocore}, AGE, NOW) == Version("1.40.0")


def test_group_without_common_version():
    boto3 = {"1.40.5": _files("2026-01-01T00:00:00Z")}
    botocore = {"1.40.3": _files("2026-01-01T00:00:00Z")}
    assert find_group_target({"boto3": boto3, "botocore": botocore}, AGE, NOW) is None


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        pytest.param("boto3>=1.41.0", "boto3>=1.43.0", id="plain"),
        pytest.param("boto3 >= 1.41.0", "boto3 >= 1.43.0", id="spaced"),
        pytest.param("boto3>1.41.0", "boto3>=1.43.0", id="exclusive"),
        pytest.param("pydantic-ai-slim[mcp]>=1.0", "pydantic-ai-slim[mcp]>=1.43.0", id="extras"),
        pytest.param(
            "boto3>=1.41.0; python_version < '3.14'", "boto3>=1.43.0; python_version < '3.14'", id="marker"
        ),
        pytest.param("boto3>=1.44.0; python_version >= '3.14'", None, id="floor-already-higher"),
        pytest.param("boto3>=1.43.0", None, id="floor-equal"),
        pytest.param("boto3", None, id="no-floor"),
    ],
)
def test_rewrite(raw, expected):
    assert rewrite_requirement(raw, Version("1.43.0")) == expected


def test_build_bump_skips_sites_already_high():
    low, high = _site("boto3>=1.41.0"), _site("boto3>=1.44.0; python_version >= '3.14'")
    bump = build_bump("boto3", [low, high], Version("1.43.0"))
    assert bump.edits == (Edit(path=low.path, old="boto3>=1.41.0", new="boto3>=1.43.0"),)
    assert bump.old_floors == ("1.41.0",)


def test_build_bump_nothing_to_raise():
    assert build_bump("boto3", [_site("boto3>=1.44.0")], Version("1.43.0")) is None


@pytest.mark.parametrize(
    "quote",
    [pytest.param('"', id="apply_bump_double_quotes"), pytest.param("'", id="apply_bump_single_quotes")],
)
def test_apply_and_revert_bump(tmp_path, quote):
    path = tmp_path / "pyproject.toml"
    original = f"dependencies = [\n    {quote}boto3>=1.41.0{quote},  # keep me\n]\n"
    path.write_text(original)
    bump = build_bump("boto3", [_site("boto3>=1.41.0", str(path))], Version("1.43.0"))
    apply_bump(bump)
    assert path.read_text() == original.replace("1.41.0", "1.43.0")
    revert_bump(bump)
    assert path.read_text() == original


def test_apply_bump_missing_text_raises(tmp_path):
    path = tmp_path / "pyproject.toml"
    path.write_text("dependencies = []\n")
    bump = build_bump("boto3", [_site("boto3>=1.41.0", str(path))], Version("1.43.0"))
    with pytest.raises(ValueError, match="boto3>=1.41.0"):
        apply_bump(bump)


def _bump(unit: str) -> Bump:
    return Bump(unit=unit, packages=(unit,), old_floors=("1.0",), target=Version("2.0"), edits=())


class FakeWorkspace:
    """Tracks which bumps are applied; the check fails while any 'bad' set is fully applied."""

    def __init__(self, bad_sets: list[set[str]], broken: bool = False):
        self.applied: set[str] = set()
        self.bad_sets = bad_sets
        self.broken = broken

    def apply(self, bump):
        self.applied.add(bump.unit)

    def revert(self, bump):
        self.applied.discard(bump.unit)

    def check(self) -> ResolveResult:
        if self.broken:
            return ResolveResult(ok=False, error="lock broken")
        for bad in self.bad_sets:
            if bad <= self.applied:
                return ResolveResult(ok=False, error=f"conflict {sorted(bad)}")
        return ResolveResult(ok=True)


@pytest.fixture
def workspace(monkeypatch):
    def _make(bad_sets, broken=False):
        ws = FakeWorkspace(bad_sets, broken)
        monkeypatch.setattr("upgrade_dependency_floors.apply_bump", ws.apply)
        monkeypatch.setattr("upgrade_dependency_floors.revert_bump", ws.revert)
        return ws

    return _make


def test_rollback_nothing_when_green(workspace):
    ws = workspace([])
    applied, rolled_back = apply_with_rollback([_bump(u) for u in "abcd"], ws.check)
    assert [b.unit for b in applied] == list("abcd")
    assert rolled_back == []
    assert ws.applied == set("abcd")


def test_rollback_single_bad(workspace):
    ws = workspace([{"c"}])
    applied, rolled_back = apply_with_rollback([_bump(u) for u in "abcdefg"], ws.check)
    assert [(b.unit, e) for b, e in rolled_back] == [("c", "conflict ['c']")]
    assert ws.applied == set("abdefg")


def test_rollback_interacting_pair(workspace):
    ws = workspace([{"b", "e"}])
    applied, rolled_back = apply_with_rollback([_bump(u) for u in "abcdef"], ws.check)
    assert ws.check().ok
    # Only one side of the conflicting pair is dropped, not the whole batch.
    assert [b.unit for b, _ in rolled_back] == ["e"]
    assert ws.applied == set("abcdf") == {b.unit for b in applied}


def test_lock_broken_before_bumps(workspace):
    ws = workspace([], broken=True)
    with pytest.raises(LockAlreadyBrokenError, match="lock broken"):
        apply_with_rollback([_bump("a")], ws.check)
    assert ws.applied == set()


@mock.patch("upgrade_dependency_floors.subprocess.run", autospec=True)
def test_resolve_check_runs_both_resolutions(mock_run, tmp_path):
    mock_run.return_value = mock.Mock(spec=subprocess.CompletedProcess, returncode=0, stderr="")
    assert resolve_check(tmp_path) == ResolveResult(ok=True)
    assert [c.args[0] for c in mock_run.call_args_list] == [list(cmd) for cmd in RESOLVE_COMMANDS]


@mock.patch("upgrade_dependency_floors.subprocess.run", autospec=True)
def test_resolve_check_reports_stderr_tail(mock_run, tmp_path):
    mock_run.return_value = mock.Mock(
        spec=subprocess.CompletedProcess, returncode=1, stderr="\n".join(f"line {i}" for i in range(50))
    )
    result = resolve_check(tmp_path)
    assert not result.ok
    assert result.error.splitlines()[-1] == "line 49"
    assert "line 0" not in result.error


ROOT_CONFIG = CONFIG + '\n[tool.uv.workspace]\nmembers = ["providers/amazon"]\n'
AMAZON = """
[project]
name = "apache-airflow-providers-amazon"
dependencies = [
    "boto3>=1.41.0",
    "botocore>=1.41.0",
    "google-cloud-storage>=2.0.0,<3",
    "google-cloud-bigquery>=3.0.0",
    "sagemaker-studio>=1.0.25",
    "apache-airflow-providers-google>=1.0",
]
"""
OLD = "2026-01-01T00:00:00Z"
PYPI = {
    "boto3": {"1.41.0": _files(OLD), "1.42.0": _files(OLD), "1.50.0": _files("2026-09-01T00:00:00Z")},
    "botocore": {"1.41.0": _files(OLD), "1.42.0": _files(OLD)},
    "google-cloud-bigquery": {"3.0.0": _files(OLD), "3.9.0": _files(OLD)},
}
WORKSPACE = frozenset({"apache-airflow-providers-google"})


@pytest.fixture
def tree(tmp_path):
    (tmp_path / "pyproject.toml").write_text(ROOT_CONFIG)
    (tmp_path / "providers/amazon").mkdir(parents=True)
    amazon = tmp_path / "providers/amazon/pyproject.toml"
    amazon.write_text(AMAZON)
    return tmp_path, amazon


def _fetch(name):
    if name not in PYPI:
        raise OSError(f"{name} not on PyPI")
    return PYPI[name]


def _green():
    return ResolveResult(ok=True)


def test_run_raises_curated_floors(tree):
    root, amazon = tree
    report = run(root, [amazon], WORKSPACE, NOW, _fetch, _green)
    assert {b.unit: str(b.target) for b in report.raised} == {
        "boto3+botocore": "1.42.0",
        "google-cloud-bigquery": "3.9.0",
    }
    text = amazon.read_text()
    assert '"boto3>=1.42.0"' in text
    assert '"botocore>=1.42.0"' in text
    assert '"google-cloud-bigquery>=3.9.0"' in text
    assert "held back by <3" in report.skipped["google-cloud-storage"]
    # not curated, so never considered
    assert "sagemaker-studio" not in report.skipped


def test_run_ignores_workspace_members(tree):
    root, amazon = tree
    (root / "pyproject.toml").write_text(
        ROOT_CONFIG.replace('"Google_Cloud-*"', '"Google_Cloud-*", "apache-airflow-*"')
    )
    report = run(root, [amazon], WORKSPACE, NOW, _fetch, _green)
    assert all("apache-airflow" not in b.unit for b in report.raised)
    assert '"apache-airflow-providers-google>=1.0"' in amazon.read_text()


def test_run_skips_package_when_fetch_fails(tree):
    root, amazon = tree
    report = run(root, [amazon], frozenset(), NOW, lambda name: _fetch("missing"), _green)
    assert report.raised == []
    assert report.skipped["google-cloud-bigquery"].startswith("PyPI metadata unavailable")
    assert amazon.read_text() == AMAZON


def test_render_report():
    bump = Bump(
        unit="boto3+botocore",
        packages=("boto3", "botocore"),
        old_floors=("1.41.0",),
        target=Version("1.42.0"),
        edits=(Edit(path=Path("providers/amazon/pyproject.toml"), old="x", new="y"),),
    )
    bad = Bump(
        unit="google-cloud-bigquery",
        packages=("google-cloud-bigquery",),
        old_floors=("3.0.0",),
        target=Version("3.9.0"),
        edits=(),
    )
    text = render_report(
        Report(raised=[bump], skipped={"pandas": "DataFrame XComs"}, rolled_back=[(bad, "boom")])
    )
    assert "### Dependency floors" in text
    assert "- `boto3`, `botocore`: 1.41.0 → 1.42.0" in text
    assert "- `pandas`: DataFrame XComs" in text
    assert "- `google-cloud-bigquery` → 3.9.0" in text
    assert "boom" in text


def test_exclusion_reason_shows_path_relative_to_repository(tmp_path):
    config = load_config(_write(tmp_path, CONFIG))
    site = _site("boto3>=1.41,<2", str(AIRFLOW_ROOT_PATH / "providers" / "amazon" / "pyproject.toml"))
    reason = get_exclusion_reason("boto3", [site], config)
    assert reason == "held back by <2 in providers/amazon/pyproject.toml"


def test_workspace_pyprojects_skip_members_with_own_lock(tmp_path):
    # A member with its own uv.lock (dev/breeze) is not covered by the root resolve check,
    # so raising its floors would leave that lock stale.
    (tmp_path / "pyproject.toml").write_text(
        '[tool.uv.workspace]\nmembers = [".", "providers/*", "dev/breeze"]\n'
    )
    for member in ("providers/amazon", "providers/google", "dev/breeze"):
        (tmp_path / member).mkdir(parents=True)
        (tmp_path / member / "pyproject.toml").write_text("[project]\n")
    (tmp_path / "dev/breeze/uv.lock").write_text("")
    assert get_workspace_pyprojects(tmp_path) == [
        tmp_path / "pyproject.toml",
        tmp_path / "providers/amazon/pyproject.toml",
        tmp_path / "providers/google/pyproject.toml",
    ]


def test_revert_restores_only_the_edited_occurrences(tmp_path):
    path = tmp_path / "pyproject.toml"
    original = (
        '[project]\ndependencies = ["google-cloud-x>=1.0"]\n'
        '[dependency-groups]\ndev = ["google-cloud-x>=2.0"]\n'
    )
    path.write_text(original)
    sites = [_site("google-cloud-x>=1.0", str(path)), _site("google-cloud-x>=2.0", str(path))]
    bump = build_bump("google-cloud-x", sites, Version("2.0"))
    apply_bump(bump)
    assert path.read_text().count('"google-cloud-x>=2.0"') == 2
    revert_bump(bump)
    # The requirement that was already at the target must not be lowered by the rollback.
    assert path.read_text() == original


def test_apply_bump_is_all_or_nothing(tmp_path):
    first, second = tmp_path / "a.toml", tmp_path / "b.toml"
    first.write_text('dependencies = ["boto3>=1.41.0"]\n')
    # The parsed requirement does not match the file text (escaped quotes), so this edit cannot apply.
    second.write_text('dependencies = ["boto3>=1.41.0; python_version < \\"3.14\\""]\n')
    bump = build_bump(
        "boto3",
        [_site("boto3>=1.41.0", str(first)), _site("boto3>=1.40.0; python_version < '3.14'", str(second))],
        Version("1.43.0"),
    )
    with pytest.raises(ValueError, match="not found"):
        apply_bump(bump)
    assert first.read_text() == 'dependencies = ["boto3>=1.41.0"]\n'


def test_failed_apply_reverts_earlier_bumps(workspace, monkeypatch):
    ws = workspace([])

    def apply_or_fail(bump):
        if bump.unit == "b":
            raise ValueError("Requirement not found")
        ws.apply(bump)

    monkeypatch.setattr("upgrade_dependency_floors.apply_bump", apply_or_fail)
    with pytest.raises(ValueError, match="not found"):
        apply_with_rollback([_bump("a"), _bump("b")], ws.check)
    assert ws.applied == set()


def test_run_skips_package_with_malformed_release_data(tree):
    root, amazon = tree

    def fetch(name):
        if name == "google-cloud-bigquery":
            return {"3.9.0": [{"yanked": False}]}
        return _fetch(name)

    report = run(root, [amazon], WORKSPACE, NOW, fetch, _green)
    assert report.skipped["google-cloud-bigquery"].startswith("PyPI metadata unavailable")
    assert {b.unit for b in report.raised} == {"boto3+botocore"}


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(ValueError("bad config"), id="config"),
        pytest.param(LockAlreadyBrokenError("bad config"), id="lock"),
    ],
)
def test_main_reports_why_floors_were_not_updated(tmp_path, monkeypatch, error):
    report_path = tmp_path / "report.md"
    monkeypatch.setenv("DEPENDENCY_FLOORS_REPORT", str(report_path))

    def fail(*args, **kwargs):
        raise error

    monkeypatch.setattr("upgrade_dependency_floors.run", fail)
    assert main() == 1
    text = report_path.read_text()
    assert "### Dependency floors" in text
    assert "Not updated: bad config" in text


def test_render_report_lists_files_of_raised_floors():
    amazon = AIRFLOW_ROOT_PATH / "providers" / "amazon" / "pyproject.toml"
    google = AIRFLOW_ROOT_PATH / "providers" / "google" / "pyproject.toml"
    bump = Bump(
        unit="boto3",
        packages=("boto3",),
        old_floors=("1.41.0",),
        target=Version("1.42.0"),
        edits=(
            Edit(path=google, old="a", new="b"),
            Edit(path=amazon, old="c", new="d"),
            Edit(path=amazon, old="e", new="f"),
        ),
    )
    text = render_report(Report(raised=[bump], skipped={}, rolled_back=[]))
    assert (
        "- `boto3`: 1.41.0 → 1.42.0 (providers/amazon/pyproject.toml, providers/google/pyproject.toml)"
        in text
    )


@pytest.mark.parametrize("table", ["constraint-dependencies", "override-dependencies"])
def test_run_respects_holds_in_root_uv_settings(tree, table):
    root, amazon = tree
    (root / "pyproject.toml").write_text(
        ROOT_CONFIG + f'\n[tool.uv]\n{table} = ["google-cloud-bigquery<3.5"]\n'
    )
    report = run(root, [amazon], WORKSPACE, NOW, _fetch, _green)
    assert report.skipped["google-cloud-bigquery"] == f"held back by <3.5 in [tool.uv] {table}"
    assert '"google-cloud-bigquery>=3.0.0"' in amazon.read_text()


MANY_GOOGLE = [f"google-cloud-p{i}" for i in range(6)]


@pytest.fixture
def many_tree(tmp_path):
    (tmp_path / "pyproject.toml").write_text(ROOT_CONFIG)
    provider = tmp_path / "pyproject.provider.toml"
    deps = ",\n".join(f'    "{name}>=1.0"' for name in MANY_GOOGLE)
    provider.write_text(f'[project]\nname = "p"\ndependencies = [\n{deps}\n]\n')
    return tmp_path, provider


def test_run_stops_asking_pypi_when_it_is_unreachable(many_tree):
    root, provider = many_tree
    calls = []

    def unreachable(name):
        calls.append(name)
        raise requests.ConnectionError("connection timed out")

    report = run(root, [provider], frozenset(), NOW, unreachable, _green)
    assert len(calls) == 3
    assert set(report.skipped) == set(MANY_GOOGLE)
    assert report.skipped[MANY_GOOGLE[-1]] == "PyPI unreachable (3 consecutive connection failures)"


def test_http_errors_do_not_count_as_unreachable(many_tree):
    root, provider = many_tree
    calls = []

    def not_found(name):
        calls.append(name)
        raise requests.HTTPError("404 Not Found")

    run(root, [provider], frozenset(), NOW, not_found, _green)
    assert len(calls) == len(MANY_GOOGLE)
