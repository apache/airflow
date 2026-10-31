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

# ruff: noqa: S101
from __future__ import annotations

import importlib.util
import io
import json
import tarfile
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest
from packaging.requirements import Requirement

SPEC = importlib.util.spec_from_file_location(
    "scan_wheel_floors", Path(__file__).parents[1] / "scan_wheel_floors.py"
)
scan = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(scan)

PYTHON = "3.15.0"
CP315_X86 = "pkg-{v}-cp315-cp315-manylinux_2_28_x86_64.whl"
CP315_ARM = "pkg-{v}-cp315-cp315-manylinux_2_28_aarch64.whl"
CP314_X86 = "pkg-{v}-cp314-cp314-manylinux_2_28_x86_64.whl"
CP314_ARM = "pkg-{v}-cp314-cp314-manylinux_2_28_aarch64.whl"

PYPROJECT = """
[project]
dependencies = [
  "msgspec>=0.19.0",
  "msgspec>=0.22.0; python_version >= '3.15'",
  "old>=1.0; python_version < '3.15'",
  "apache-airflow-task-sdk>=1.0",
  "pinned @ https://example.com/pinned-1.0-py3-none-any.whl",
]
[project.optional-dependencies]
extra = ["gevent>=25.4.1"]
[dependency-groups]
dev = ["time-machine>=3.0.0", {include-group = "other"}]
[tool.uv]
override-dependencies = ["numpy>=2.5.2; python_version == '3.15'"]
constraint-dependencies = ["lazy-object-proxy<1.12"]
"""


def wheel(template: str, version: str, **extra) -> dict:
    return {
        "filename": template.format(v=version),
        "packagetype": "bdist_wheel",
        "upload_time_iso_8601": extra.pop("uploaded", "2026-01-01T00:00:00Z"),
    } | extra


def sdist(version: str) -> dict:
    return {
        "filename": f"pkg-{version}.tar.gz",
        "packagetype": "sdist",
        "url": f"https://files.example/pkg-{version}.tar.gz",
    }


def tarball(*names: str) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
        for name in names:
            info = tarfile.TarInfo(name)
            archive.addfile(info, io.BytesIO(b""))
    return buffer.getvalue()


@pytest.fixture(autouse=True)
def compiled_sdists():
    """Treat every sdist as compiled unless a test says otherwise."""
    with mock.patch.object(
        scan.audit_wheels, "fetch_bytes", return_value=tarball("pkg/ext.c", "pkg/__init__.py")
    ) as fetch:
        yield fetch


def both_architectures(version: str, **extra) -> list[dict]:
    return [wheel(CP315_X86, version, **extra), wheel(CP315_ARM, version, **extra)]


def declared(*lines: str) -> list[tuple[Requirement, str]]:
    return [(Requirement(line), "x/pyproject.toml") for line in lines]


def release_data(**releases: list[dict]) -> dict:
    return {"releases": {version.replace("_", "."): files for version, files in releases.items()}}


def test_collects_every_direct_declaration_and_skips_what_does_not_apply(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(PYPROJECT, encoding="utf-8")

    collected = scan.collect_requirements([pyproject], PYTHON)

    assert sorted(collected) == ["gevent", "msgspec", "numpy", "time-machine"]
    assert [str(requirement.specifier) for requirement, _ in collected["msgspec"]] == [">=0.19.0", ">=0.22.0"]


def test_marker_for_another_machine_still_applies_when_one_architecture_matches(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "[project]\ndependencies = ['arm-only>=1; platform_machine == \"aarch64\"']\n", encoding="utf-8"
    )

    assert "arm-only" in scan.collect_requirements([pyproject], PYTHON)


def test_floor_that_already_has_wheels_is_ok():
    data = release_data(**{"1_0": both_architectures("1.0")})

    record = scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)

    assert record["status"] == "ok"
    assert record["lowest_selected"] == "1.0"


def test_proposes_first_release_with_wheels_for_both_architectures():
    data = release_data(
        **{
            "1_0": [wheel(CP314_X86, "1.0"), wheel(CP314_ARM, "1.0"), sdist("1.0")],
            "1_1": [wheel(CP315_X86, "1.1")],
            "1_2": [
                wheel(CP315_X86, "1.2", uploaded="2026-05-20T10:00:00Z"),
                wheel(CP315_ARM, "1.2", uploaded="2026-05-21T10:00:00Z"),
            ],
            "1_3": both_architectures("1.3"),
        }
    )

    record = scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)

    assert record["status"] == "raise"
    assert record["first_wheel_version"] == "1.2"
    assert record["first_wheel_uploaded"] == "2026-05-20T10:00:00Z"
    assert record["proposal"] == 'pkg>=1.2; python_version >= "3.15"'


def test_pure_python_wheel_counts():
    data = release_data(**{"1_0": [wheel("pkg-{v}-py3-none-any.whl", "1.0")]})

    assert scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)["status"] == "ok"


def test_prereleases_yanked_files_and_requires_python_misses_are_skipped():
    data = release_data(
        **{
            "1_0": [sdist("1.0")],
            "1_1rc1": both_architectures("1.1rc1"),
            "1_2": both_architectures("1.2", yanked=True),
            "1_3": both_architectures("1.3", requires_python="<3.15"),
            "1_4": both_architectures("1.4", requires_python=">=3.10"),
        }
    )

    record = scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)

    assert record["first_wheel_version"] == "1.4"


def test_every_declaration_counts_towards_the_floor_and_the_cap():
    data = release_data(
        **{
            "1_0": [sdist("1.0")],
            "1_5": [sdist("1.5")],
            "2_0": both_architectures("2.0"),
            "3_0": both_architectures("3.0"),
        }
    )

    record = scan.scan_package("pkg", declared("pkg>=1.0", "pkg>=1.5", "pkg<3"), PYTHON, data)

    assert record["lowest_selected"] == "1.5"
    assert record["first_wheel_version"] == "2.0"


def test_wheels_only_beyond_a_cap_are_blocked():
    data = release_data(**{"1_0": [sdist("1.0")], "2_0": both_architectures("2.0")})

    record = scan.scan_package("pkg", declared("pkg>=1.0,<2"), PYTHON, data)

    assert record["status"] == "blocked-by-cap"
    assert record["first_wheel_version"] == "2.0"


def test_no_wheel_anywhere_is_source_only():
    data = release_data(**{"1_0": [sdist("1.0")], "1_1": [sdist("1.1")]})

    assert scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)["status"] == "source-only"


def test_requirement_matching_no_release_is_unsatisfiable():
    data = release_data(**{"1_0": [sdist("1.0")]})

    assert scan.scan_package("pkg", declared("pkg>=2.0"), PYTHON, data)["status"] == "unsatisfiable"


@pytest.mark.parametrize(
    ("requirement", "unbounded"),
    [("pkg", True), ("pkg<2", True), ("pkg>=1.0", False), ("pkg==1.0", False), ("pkg~=1.0", False)],
)
def test_flags_requirements_without_a_lower_bound(requirement, unbounded):
    data = release_data(**{"1_0": both_architectures("1.0")})

    assert scan.scan_package("pkg", declared(requirement), PYTHON, data)["unbounded"] is unbounded


def test_main_writes_json_and_markdown_and_reports_lookup_errors(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "[project]\ndependencies = ['good>=1.0', 'stale>=1.0', 'missing>=1.0']\n", encoding="utf-8"
    )
    releases = {
        "good": release_data(**{"1_0": both_architectures("1.0")}),
        "stale": release_data(**{"1_0": [sdist("1.0")], "1_1": both_architectures("1.1")}),
    }

    def fake_fetch(name):
        if name not in releases:
            raise URLError("not found")
        return releases[name]

    output, table = tmp_path / "scan.json", tmp_path / "scan.md"
    with mock.patch.object(scan.audit_wheels, "fetch_release", side_effect=fake_fetch):
        exit_code = scan.main(
            [
                "--python-version",
                PYTHON,
                "--pyproject",
                str(pyproject),
                "--output",
                str(output),
                "--markdown",
                str(table),
            ]
        )

    statuses = {record["name"]: record["status"] for record in json.loads(output.read_text())["packages"]}
    assert statuses == {"good": "ok", "stale": "raise", "missing": "lookup-error"}
    assert exit_code == 1
    assert 'stale>=1.1; python_version >= "3.15"' in table.read_text()
    assert "good" not in table.read_text()


def test_main_rejects_a_non_patch_python_version(tmp_path):
    with pytest.raises(SystemExit):
        scan.main(["--python-version", "3.15", "--pyproject", "x", "--output", str(tmp_path / "o.json")])


def test_pure_python_sdist_at_the_floor_needs_no_raised_floor(compiled_sdists):
    compiled_sdists.return_value = tarball("pkg/__init__.py")
    data = release_data(**{"1_0": [sdist("1.0")], "1_1": both_architectures("1.1")})

    record = scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)

    assert record["status"] == "pure-python-sdist"
    assert "proposal" not in record


def test_compiled_sources_at_the_floor_are_recorded_with_the_proposal():
    data = release_data(**{"1_0": [sdist("1.0")], "1_1": both_architectures("1.1")})

    record = scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)

    assert record["status"] == "raise"
    assert record["compiled_source_evidence"] == ["pkg/ext.c"]


def test_wheels_in_releases_older_than_the_floor_are_not_a_cap_problem():
    data = release_data(**{"0_5": both_architectures("0.5"), "1_0": [sdist("1.0")]})

    assert scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)["status"] == "source-only"


def test_prereleases_are_considered_only_when_a_specifier_names_one():
    data = release_data(**{"0_47b0": both_architectures("0.47b0"), "0_48b0": both_architectures("0.48b0")})

    assert scan.scan_package("pkg", declared("pkg>=0.47b0"), PYTHON, data)["status"] == "ok"
    assert scan.scan_package("pkg", declared("pkg>=0.47"), PYTHON, data)["status"] == "unsatisfiable"


def test_lookup_is_retried_once():
    data = release_data(**{"1_0": both_architectures("1.0")})
    with mock.patch.object(scan.audit_wheels, "fetch_release", side_effect=[URLError("flaky"), data]):
        record = scan.scan_item("pkg", declared("pkg>=1.0"), PYTHON)

    assert record["status"] == "ok"


def test_pure_python_fallback_wheel_of_a_compiled_project_does_not_count():
    data = release_data(
        **{
            "1_0": [sdist("1.0")],
            "1_1": [
                wheel(CP314_X86, "1.1"),
                wheel(CP314_ARM, "1.1"),
                wheel("pkg-{v}-py3-none-any.whl", "1.1"),
            ],
        }
    )

    assert scan.scan_package("pkg", declared("pkg>=1.0"), PYTHON, data)["status"] == "source-only"


def test_releases_with_malformed_requires_python_are_skipped():
    data = release_data(
        **{
            "0_9": [wheel(CP315_X86, "0.9", requires_python=">=3.4.*")],
            "1_0": both_architectures("1.0"),
        }
    )

    assert scan.scan_package("pkg", declared("pkg"), PYTHON, data)["lowest_selected"] == "1.0"
