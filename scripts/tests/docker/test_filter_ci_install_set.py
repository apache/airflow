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

import importlib.util
import sys
from pathlib import Path

import pytest
from packaging.specifiers import SpecifierSet
from packaging.version import Version

SCRIPT = Path(__file__).resolve().parents[2] / "docker" / "filter_ci_install_set.py"
_spec = importlib.util.spec_from_file_location("filter_ci_install_set", SCRIPT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load {SCRIPT}")
filter_ci_install_set = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(filter_ci_install_set)


def _write_manifest(directory: Path, name: str, requires_python: str | None = ">=3.11") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    manifest = f'[project]\nname = "{name}"\n'
    if requires_python is not None:
        manifest += f'requires-python = "{requires_python}"\n'
    (directory / "pyproject.toml").write_text(manifest, encoding="utf-8")


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    """A workspace holding only manifests, as the image's dependency-manifests stage does."""
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow"\n'
        # The root and airflow-core declare exclusions of their own; only providers may ever be dropped.
        'requires-python = ">=3.11,!=3.15"\n'
        '[tool.uv.workspace]\nmembers = [".", "airflow-core", "providers/kept", "providers/kafka", '
        '"providers/cassandra"]\n',
        encoding="utf-8",
    )
    _write_manifest(root / "airflow-core", "apache-airflow-core", ">=3.11,!=3.15.*")
    _write_manifest(root / "providers" / "kept", "apache-airflow-providers-kept")
    _write_manifest(root / "providers" / "kafka", "apache-airflow-providers-apache-kafka", ">=3.11,!=3.15.*")
    _write_manifest(
        root / "providers" / "cassandra", "apache-airflow-providers-cassandra", ">=3.11,!=3.14.*,!=3.15.*"
    )
    return root


@pytest.mark.parametrize(
    "requires_python",
    [">=3.11", ">=3.11,!=3.14.*", ">=3.11, !=3.13.1.*", ">=3.11.2,!=3.15.*", ">=3.12"],
)
@pytest.mark.parametrize(
    "python_version",
    ["3.10.12", "3.11.0", "3.11.1", "3.11.2", "3.13.1", "3.13.2", "3.13.10", "3.14.0", "3.15.0"],
)
def test_is_python_version_allowed_agrees_with_pep_440(requires_python: str, python_version: str):
    release = tuple(int(part) for part in python_version.split("."))

    assert filter_ci_install_set.is_python_version_allowed(requires_python, release) == SpecifierSet(
        requires_python
    ).contains(Version(python_version))


@pytest.mark.parametrize(
    "clause",
    ["<3.14", "~=3.11", "==3.12.*", "!=3.15", ">=3.11.*", ">3.10", "!=3.15.0rc1.*", ""],
)
def test_is_python_version_allowed_rejects_clauses_breeze_does_not_write(clause: str):
    """An unknown clause must stop the build: ignoring it would keep a provider that cannot be installed."""
    with pytest.raises(ValueError, match="Unsupported requires-python clause"):
        filter_ci_install_set.is_python_version_allowed(f">=3.11,{clause}", (3, 14, 0))


@pytest.mark.parametrize(
    ("python_version", "expected"),
    [
        ((3, 15, 0), {"apache-airflow-providers-apache-kafka", "apache-airflow-providers-cassandra"}),
        ((3, 14, 2), {"apache-airflow-providers-cassandra"}),
        ((3, 13, 5), set()),
    ],
)
def test_split_members_excludes_providers_whose_requires_python_rejects_the_interpreter(
    workspace: Path, python_version: tuple[int, ...], expected: set[str]
):
    kept, excluded = filter_ci_install_set.split_members(workspace, python_version)

    assert excluded == expected
    assert (
        set(kept)
        == {
            "apache-airflow",
            "apache-airflow-core",
            "apache-airflow-providers-kept",
            "apache-airflow-providers-apache-kafka",
            "apache-airflow-providers-cassandra",
        }
        - expected
    )


def test_split_members_keeps_a_provider_without_requires_python(workspace: Path):
    _write_manifest(
        workspace / "providers" / "kafka", "apache-airflow-providers-apache-kafka", requires_python=None
    )

    kept, excluded = filter_ci_install_set.split_members(workspace, (3, 15, 0))

    assert "apache-airflow-providers-apache-kafka" in kept
    assert excluded == {"apache-airflow-providers-cassandra"}


def test_split_members_names_the_manifest_and_clause_it_cannot_evaluate(workspace: Path):
    _write_manifest(
        workspace / "providers" / "kafka", "apache-airflow-providers-apache-kafka", ">=3.11,<3.15"
    )

    with pytest.raises(ValueError, match=r"providers/kafka/pyproject\.toml.*'<3\.15'"):
        filter_ci_install_set.split_members(workspace, (3, 14, 0))


@pytest.mark.parametrize(
    ("manifest", "message"),
    [
        ("[project\n", r"Cannot parse .*providers/kafka/pyproject\.toml"),
        ('[project]\nrequires-python = ">=3.11"\n', r"No project\.name in .*providers/kafka/pyproject\.toml"),
    ],
)
def test_split_members_names_a_manifest_it_cannot_read(workspace: Path, manifest: str, message: str):
    (workspace / "providers" / "kafka" / "pyproject.toml").write_text(manifest, encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        filter_ci_install_set.split_members(workspace, (3, 14, 0))


def test_find_workspace_members_rejects_a_missing_member(tmp_path: Path):
    with pytest.raises(ValueError, match="does not exist"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["missing"])


def test_find_workspace_members_rejects_a_member_without_manifest(tmp_path: Path):
    (tmp_path / "member").mkdir()
    with pytest.raises(ValueError, match="no pyproject.toml"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["member"])


@pytest.mark.parametrize(
    ("python_version", "expected_code"),
    [
        ("3.15.0", filter_ci_install_set.EXIT_HAS_EXCLUDED_MEMBERS),
        ("3.13.5", filter_ci_install_set.EXIT_NO_EXCLUDED_MEMBERS),
    ],
)
def test_main_probe_reports_distinct_codes(workspace: Path, python_version: str, expected_code: int):
    """The probe's two answers must not share a code with a crash, which exits non-zero otherwise."""
    assert (
        filter_ci_install_set.main(["--root", str(workspace), "--python-version", python_version, "--probe"])
        == expected_code
    )


@pytest.mark.parametrize("excludes_running_interpreter", [True, False])
def test_main_defaults_to_the_running_interpreter(workspace: Path, excludes_running_interpreter: bool):
    """With no version given, the interpreter running the helper - the image's own - decides."""
    running = f"{sys.version_info.major}.{sys.version_info.minor}"
    other = f"{sys.version_info.major}.{sys.version_info.minor + 1}"
    for provider in ("kafka", "cassandra"):
        _write_manifest(
            workspace / "providers" / provider,
            f"apache-airflow-providers-{provider}",
            f">=3.11,!={running if excludes_running_interpreter else other}.*",
        )

    assert filter_ci_install_set.main(["--root", str(workspace), "--probe"]) == (
        filter_ci_install_set.EXIT_HAS_EXCLUDED_MEMBERS
        if excludes_running_interpreter
        else filter_ci_install_set.EXIT_NO_EXCLUDED_MEMBERS
    )


def test_main_writes_kept_packages_and_editables(workspace: Path, tmp_path: Path):
    packages = tmp_path / "packages.txt"
    editables = tmp_path / "editables.txt"

    exit_code = filter_ci_install_set.main(
        [
            "--root",
            str(workspace),
            "--python-version",
            "3.15.0",
            "--packages",
            str(packages),
            "--editables",
            str(editables),
        ]
    )

    assert exit_code == 0
    # One entry per line: the shell reads both files with mapfile.
    assert packages.read_text(encoding="utf-8").splitlines() == [
        "apache-airflow",
        "apache-airflow-core",
        "apache-airflow-providers-kept",
    ]
    written = {Path(line).resolve() for line in editables.read_text(encoding="utf-8").splitlines()}
    assert written == {
        workspace.resolve(),
        (workspace / "airflow-core").resolve(),
        (workspace / "providers" / "kept").resolve(),
    }
