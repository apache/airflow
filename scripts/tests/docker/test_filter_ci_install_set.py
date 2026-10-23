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
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "docker" / "filter_ci_install_set.py"
_spec = importlib.util.spec_from_file_location("filter_ci_install_set", SCRIPT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load {SCRIPT}")
filter_ci_install_set = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(filter_ci_install_set)


def _write_manifest(directory: Path, name: str, requires_python: str = ">=3.11") -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(
        f'[project]\nname = "{name}"\nrequires-python = "{requires_python}"\n', encoding="utf-8"
    )


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    root.mkdir()
    (root / "pyproject.toml").write_text(
        '[project]\nname = "apache-airflow"\n'
        # The root declares an exclusion of its own; only providers may ever be dropped.
        'requires-python = ">=3.11,!=3.15.*"\n'
        '[tool.uv.workspace]\nmembers = [".", "airflow-core", "providers/*"]\n',
        encoding="utf-8",
    )
    _write_manifest(root / "airflow-core", "apache-airflow-core", ">=3.11,!=3.15.*")
    _write_manifest(root / "providers" / "kept", "apache-airflow-providers-kept")
    _write_manifest(root / "providers" / "kafka", "apache-airflow-providers-apache-kafka")
    (root / "generated").mkdir()
    (root / "generated" / "provider_dependencies.json").write_text(
        json.dumps(
            {
                "kept": {"excluded-python-versions": []},
                "apache.kafka": {"excluded-python-versions": ["3.15"]},
                "cassandra": {"excluded-python-versions": ["3.14", "3.15"]},
            }
        ),
        encoding="utf-8",
    )
    return root


@pytest.mark.parametrize(
    ("provider_id", "expected"),
    [
        ("kept", "apache-airflow-providers-kept"),
        ("apache.kafka", "apache-airflow-providers-apache-kafka"),
        ("common.sql", "apache-airflow-providers-common-sql"),
    ],
)
def test_build_provider_distribution_name(provider_id: str, expected: str):
    assert filter_ci_install_set.build_provider_distribution_name(provider_id) == expected


@pytest.mark.parametrize(
    ("minor", "expected"),
    [
        ("3.15", {"apache-airflow-providers-apache-kafka", "apache-airflow-providers-cassandra"}),
        ("3.14", {"apache-airflow-providers-cassandra"}),
        ("3.13", set()),
    ],
)
def test_read_excluded_distributions_selects_only_the_target_minor(
    workspace: Path, minor: str, expected: set[str]
):
    assert filter_ci_install_set.read_excluded_distributions(workspace, minor) == expected


def test_split_members_excludes_only_providers_listed_for_the_minor(workspace: Path):
    kept, excluded = filter_ci_install_set.split_members(workspace, "3.15")

    assert excluded == {"apache-airflow-providers-apache-kafka"}
    assert set(kept) == {
        "apache-airflow",
        "apache-airflow-core",
        "apache-airflow-providers-kept",
    }


def test_split_members_keeps_core_members_whose_requires_python_excludes_the_minor(workspace: Path):
    """Only providers appear in the generated metadata, so a core member cannot silently drop out.

    Installing without Airflow itself would otherwise look like a successful build.
    """
    kept, _ = filter_ci_install_set.split_members(workspace, "3.15")

    assert "apache-airflow" in kept
    assert "apache-airflow-core" in kept


def test_find_workspace_members_rejects_missing_literal_member(tmp_path: Path):
    with pytest.raises(ValueError, match="does not exist"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["missing"])


def test_find_workspace_members_rejects_literal_member_without_manifest(tmp_path: Path):
    (tmp_path / "member").mkdir()
    with pytest.raises(ValueError, match="no pyproject.toml"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["member"])


@pytest.mark.parametrize(
    ("minor", "expected_code"),
    [
        ("3.15", filter_ci_install_set.EXIT_HAS_EXCLUDED_MEMBERS),
        ("3.13", filter_ci_install_set.EXIT_NO_EXCLUDED_MEMBERS),
    ],
)
def test_main_probe_reports_distinct_codes(workspace: Path, minor: str, expected_code: int):
    """The probe's two answers must not share a code with a crash, which exits non-zero otherwise."""
    assert (
        filter_ci_install_set.main(["--root", str(workspace), "--python-minor", minor, "--probe"])
        == expected_code
    )


def test_main_probe_fails_loudly_when_the_metadata_cannot_be_read(workspace: Path):
    (workspace / "generated" / "provider_dependencies.json").unlink()

    with pytest.raises(FileNotFoundError):
        filter_ci_install_set.main(["--root", str(workspace), "--python-minor", "3.15", "--probe"])


def test_main_writes_kept_packages_and_editables(workspace: Path, tmp_path: Path):
    packages = tmp_path / "packages.txt"
    editables = tmp_path / "editables.txt"

    exit_code = filter_ci_install_set.main(
        [
            "--root",
            str(workspace),
            "--python-minor",
            "3.15",
            "--packages",
            str(packages),
            "--editables",
            str(editables),
        ]
    )

    assert exit_code == 0
    assert packages.read_text(encoding="utf-8").split() == [
        "apache-airflow",
        "apache-airflow-core",
        "apache-airflow-providers-kept",
    ]
    written = {Path(line).resolve() for line in editables.read_text(encoding="utf-8").split()}
    assert written == {
        workspace.resolve(),
        (workspace / "airflow-core").resolve(),
        (workspace / "providers" / "kept").resolve(),
    }
    # The excluded provider must reach neither list, or uv would export its closure anyway.
    assert "kafka" not in packages.read_text(encoding="utf-8")
    assert "kafka" not in editables.read_text(encoding="utf-8")
