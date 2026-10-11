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
import zipfile
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "docker" / "filter_ci_install_set.py"
_spec = importlib.util.spec_from_file_location("filter_ci_install_set", SCRIPT)
if _spec is None or _spec.loader is None:
    raise ImportError(f"Cannot load {SCRIPT}")
filter_ci_install_set = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(filter_ci_install_set)


def _write_manifest(directory: Path, body: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "pyproject.toml").write_text(body, encoding="utf-8")


@pytest.fixture
def workspace(tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    _write_manifest(
        root,
        """
[project]
name = "apache-airflow"
requires-python = ">=3.11,!=3.16"

[dependency-groups]
dev = ["pytest"]
ci-image = [{ include-group = "dev" }, "apache-airflow-providers-excluded; python_version != \\"3.15\\""]

[tool.uv.workspace]
members = [".", "providers/kept", "providers/excluded"]
""",
    )
    _write_manifest(
        root / "providers" / "kept",
        """
[project]
name = "apache-airflow-providers-kept"
requires-python = ">=3.11,!=3.16"

[dependency-groups]
dev = ["apache-airflow-devel-common[docs]", "pytest-kept>=1.0"]
""",
    )
    _write_manifest(
        root / "providers" / "excluded",
        """
[project]
name = "apache-airflow-providers-excluded"
requires-python = ">=3.11,!=3.15.*"

[dependency-groups]
dev = ["pytest-excluded"]
""",
    )
    return root


@pytest.mark.parametrize(
    ("requires_python", "expected"),
    [
        (">=3.11,!=3.15.*", True),
        (">=3.11,!=3.15", True),
        (">=3.11, != 3.15.*", True),
        (">=3.11,!=3.150", False),
        (">=3.11,!=3.15.1", False),
        (">=3.11,!=3.16", False),
        (">=3.11", False),
    ],
)
def test_is_python_minor_excluded(requires_python: str, expected: bool):
    assert filter_ci_install_set.is_python_minor_excluded(requires_python, "3.15") is expected


@pytest.mark.parametrize(
    ("entry", "expected"),
    [
        ("Alpha>=1.0", "alpha"),
        ('beta_gamma[extra]==2.0; python_version != "3.15"', "beta-gamma"),
        ("  zeta", "zeta"),
    ],
)
def test_requirement_name_normalizes_the_distribution_name(entry: str, expected: str):
    assert filter_ci_install_set.requirement_name(entry) == expected


def test_expand_group_follows_include_group_and_keeps_markers():
    groups = {
        "dev": ['Alpha>=1.0; python_version != "3.15"', {"include-group": "docs"}],
        "docs": ["beta[extra]==2.0"],
    }

    assert filter_ci_install_set.expand_group(groups, "dev") == [
        'Alpha>=1.0; python_version != "3.15"',
        "beta[extra]==2.0",
    ]


def test_find_workspace_members_rejects_missing_literal_member(tmp_path: Path):
    with pytest.raises(ValueError, match="does not exist"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["providers/missing"])


def test_find_workspace_members_rejects_literal_member_without_manifest(tmp_path: Path):
    (tmp_path / "providers" / "bare").mkdir(parents=True)

    with pytest.raises(ValueError, match="has no pyproject.toml"):
        filter_ci_install_set.find_workspace_members(tmp_path, ["providers/bare"])


def test_split_members_separates_excluded_members(workspace: Path):
    kept, excluded = filter_ci_install_set.split_members(workspace, "3.15")

    assert sorted(kept) == ["apache-airflow", "apache-airflow-providers-kept"]
    assert excluded == {"apache-airflow-providers-excluded"}


def test_build_requirement_input_uses_local_members_and_drops_excluded_requirements(workspace: Path):
    kept, excluded = filter_ci_install_set.split_members(workspace, "3.15")

    requirements = filter_ci_install_set.build_requirement_input(workspace, kept, excluded)

    assert f"apache-airflow-providers-kept @ {(workspace / 'providers' / 'kept').as_uri()}" in requirements
    assert "apache-airflow-devel-common[docs]" in requirements
    assert "pytest-kept>=1.0" in requirements
    assert "pytest" in requirements
    assert not any(entry.startswith("apache-airflow-providers-excluded") for entry in requirements)
    assert "pytest-excluded" not in requirements


def test_split_resolved_reads_editable_and_named_member_references(tmp_path: Path):
    member = tmp_path / "with space" / "member"
    lines = [
        "# comment",
        f"-e {member.as_uri().replace(' ', '%20')}",
        f"named-member @ {(tmp_path / 'other').as_uri()}",
        "pytest==8.0.0",
    ]

    pins, member_dirs = filter_ci_install_set.split_resolved(lines)

    assert pins == ["pytest==8.0.0"]
    assert member_dirs == [member, tmp_path / "other"]


def test_split_resolved_rejects_unrecognised_local_references():
    with pytest.raises(ValueError, match="Cannot read a local member reference"):
        filter_ci_install_set.split_resolved(["-r file:///tmp/requirements.txt"])


LOCK_PINS = """\
alpha==1.0
    # via apache-airflow
beta==2.0
sphinx-theme @ https://example.org/sphinx_theme-0.3.whl
"""


def test_verify_resolved_pins_accepts_locked_versions():
    filter_ci_install_set.verify_resolved_pins(
        ["alpha==1.0", "sphinx-theme @ https://example.org/sphinx_theme-0.3.whl"], LOCK_PINS, excluded=set()
    )


def test_verify_resolved_pins_rejects_versions_outside_the_lock():
    with pytest.raises(ValueError, match="alpha==2.0 differs from the locked versions"):
        filter_ci_install_set.verify_resolved_pins(["alpha==2.0"], LOCK_PINS, excluded=set())


def test_verify_resolved_pins_rejects_packages_absent_from_the_lock():
    with pytest.raises(ValueError, match="gamma is not in the lock"):
        filter_ci_install_set.verify_resolved_pins(["gamma==1.0"], LOCK_PINS, excluded=set())


def test_verify_resolved_pins_rejects_excluded_members_resolved_from_an_index():
    with pytest.raises(ValueError, match="excluded member apache-airflow-providers-google"):
        filter_ci_install_set.verify_resolved_pins(
            ["apache-airflow-providers-google==10.0.0"],
            LOCK_PINS + "apache-airflow-providers-google==10.0.0\n",
            excluded={"apache-airflow-providers-google"},
        )


def test_main_writes_pins_and_editables_for_kept_members(workspace: Path, tmp_path: Path, monkeypatch):
    constraints = tmp_path / "pins.txt"
    constraints.write_text(LOCK_PINS + "pytest==8.0.0\n", encoding="utf-8")
    kept_dir = (workspace / "providers" / "kept").resolve()
    root_dir = workspace.resolve()
    captured: dict[str, object] = {}

    def fake_resolve(requirements, constraints_path, minor, platform, workdir, root, find_links):
        captured.update(platform=platform, find_links=find_links, root=root, requirements=requirements)
        return [
            f"-e {root_dir.as_uri()}",
            f"-e {kept_dir.as_uri()}",
            "alpha==1.0",
            "pytest==8.0.0",
        ]

    monkeypatch.setattr(filter_ci_install_set, "resolve_requirements", fake_resolve)
    output = tmp_path / "out" / "requirements.txt"
    output.parent.mkdir()
    editables = tmp_path / "editables.txt"
    wheels = tmp_path / "wheels"
    wheels.mkdir()

    exit_code = filter_ci_install_set.main(
        [
            "--root",
            str(workspace),
            "--constraints",
            str(constraints),
            "--python-minor",
            "3.15",
            "--platform",
            "aarch64",
            "--workdir",
            str(tmp_path / "work"),
            "--find-links",
            str(wheels),
            "--output",
            str(output),
            "--editables",
            str(editables),
        ]
    )

    assert exit_code == 0
    assert captured["platform"] == "aarch64-manylinux_2_28"
    assert captured["find_links"] == [wheels.resolve()]
    assert captured["root"] == root_dir
    assert output.read_text(encoding="utf-8") == "alpha==1.0\npytest==8.0.0\n"
    assert sorted(Path(line).resolve() for line in editables.read_text(encoding="utf-8").split()) == sorted(
        [kept_dir, root_dir]
    )


def test_main_rejects_malformed_python_minor(tmp_path: Path):
    with pytest.raises(SystemExit):
        filter_ci_install_set.main(
            [
                "--root",
                str(tmp_path),
                "--constraints",
                str(tmp_path),
                "--python-minor",
                "3.15.0",
                "--platform",
                "x86_64",
                "--workdir",
                str(tmp_path / "work"),
                "--output",
                str(tmp_path / "o"),
                "--editables",
                str(tmp_path / "e"),
            ]
        )


def _write_resolver_member(directory: Path, name: str, dependencies: list[str], extra_manifest: str = ""):
    _write_manifest(
        directory,
        f"""
[build-system]
requires = []
build-backend = "fixture_backend"
backend-path = ["."]

[project]
name = "{name}"
version = "1.0"
dependencies = {json.dumps(dependencies)}
{extra_manifest}
""",
    )
    metadata = f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0\n" + "".join(
        f"Requires-Dist: {dependency}\n" for dependency in dependencies
    )
    dist_info = f"{name.replace('-', '_')}-1.0.dist-info"
    (directory / "fixture_backend.py").write_text(
        "from pathlib import Path\n"
        "def prepare_metadata_for_build_wheel(metadata_directory, config_settings=None):\n"
        f"    directory = Path(metadata_directory) / {dist_info!r}\n"
        "    directory.mkdir()\n"
        f"    (directory / 'METADATA').write_text({metadata!r})\n"
        "    return directory.name\n",
        encoding="utf-8",
    )


def _write_resolver_wheel(directory: Path, name: str, metadata: str = ""):
    wheel_name = name.replace("-", "_")
    dist_info = f"{wheel_name}-1.0.dist-info"
    with zipfile.ZipFile(directory / f"{wheel_name}-1.0-py3-none-any.whl", "w") as wheel:
        wheel.writestr(
            f"{dist_info}/METADATA",
            f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0\n{metadata}\n",
        )
        wheel.writestr(f"{dist_info}/WHEEL", "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n")
        wheel.writestr(f"{dist_info}/RECORD", "")


def test_main_resolves_only_kept_dependencies_with_extras_and_python_markers(tmp_path: Path, monkeypatch):
    root = tmp_path / "workspace"
    kept = root / "providers" / "kept"
    excluded = root / "providers" / "excluded"
    _write_resolver_member(
        root,
        "test-root",
        ["shared==1.0"],
        """
[dependency-groups]
ci-image = ["extra-package[docs]", "apache-airflow-providers-excluded"]
[tool.uv.sources]
apache-airflow-providers-excluded = {workspace = true}
[tool.uv.workspace]
members = [".", "providers/kept", "providers/excluded"]
""",
    )
    _write_resolver_member(kept, "test-kept", ["shared==1.0"])
    _write_resolver_member(
        excluded,
        "apache-airflow-providers-excluded",
        ["shared==1.0", "excluded-only==1.0"],
        'requires-python = ">=3.11,!=3.15.*"',
    )
    wheels = tmp_path / "wheels"
    wheels.mkdir()
    for name in ("shared", "excluded-only", "docs-only", "wrong-python"):
        _write_resolver_wheel(wheels, name)
    _write_resolver_wheel(
        wheels,
        "extra-package",
        "Provides-Extra: docs\n"
        'Requires-Dist: docs-only==1.0; extra == "docs" and python_version == "3.15"\n'
        'Requires-Dist: wrong-python==1.0; extra == "docs" and python_version < "3.15"\n',
    )
    constraints = tmp_path / "pins.txt"
    constraints.write_text(
        "shared==1.0\nexcluded-only==1.0\nextra-package==1.0\ndocs-only==1.0\nwrong-python==1.0\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("UV_NO_INDEX", "1")
    monkeypatch.setenv("UV_OFFLINE", "1")
    monkeypatch.setenv("UV_CACHE_DIR", str(tmp_path / "uv-cache"))
    output = tmp_path / "requirements.txt"
    editables = tmp_path / "editables.txt"

    assert (
        filter_ci_install_set.main(
            [
                "--root",
                str(root),
                "--constraints",
                str(constraints),
                "--python-minor",
                "3.15",
                "--platform",
                "x86_64",
                "--workdir",
                str(tmp_path / "work"),
                "--find-links",
                str(wheels),
                "--output",
                str(output),
                "--editables",
                str(editables),
            ]
        )
        == 0
    )
    assert set(output.read_text(encoding="utf-8").splitlines()) == {
        "shared==1.0",
        "extra-package==1.0",
        "docs-only==1.0",
    }
    assert set(editables.read_text(encoding="utf-8").splitlines()) == {str(root), str(kept)}


def test_resolve_requirements_passes_absolute_paths_when_given_relative_ones(tmp_path: Path, monkeypatch):
    commands: list[list[str]] = []

    def fake_run(command, **kwargs):
        commands.append(command)
        (tmp_path / "work" / "resolved.txt").write_text("", encoding="utf-8")

    monkeypatch.setattr(filter_ci_install_set.subprocess, "run", fake_run)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "work").mkdir()
    (tmp_path / "pins.txt").write_text("", encoding="utf-8")

    filter_ci_install_set.resolve_requirements(
        ["shared @ file:///x"], Path("pins.txt"), "3.15", "x86_64-manylinux_2_28", Path("work"), tmp_path, []
    )

    (command,) = commands
    assert Path(command[3]).is_absolute()
    assert Path(command[command.index("--constraints") + 1]).is_absolute()
    assert Path(command[command.index("--output-file") + 1]).is_absolute()
