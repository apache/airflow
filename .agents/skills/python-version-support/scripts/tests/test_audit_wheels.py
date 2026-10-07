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
import json
import tarfile
import zipfile
from io import BytesIO
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

SPEC = importlib.util.spec_from_file_location("audit_wheels", Path(__file__).parents[1] / "audit_wheels.py")
wheels = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(wheels)


@pytest.mark.parametrize(
    ("tags", "architecture", "expected"),
    [
        ("py3-none-any", "x86_64", True),
        ("py2.py3-none-any", "aarch64", True),
        ("cp315-cp315-manylinux_2_28_x86_64", "x86_64", True),
        ("cp315-cp315-musllinux_1_2_aarch64", "aarch64", True),
        ("cp39-abi3-manylinux2014_aarch64", "aarch64", True),
        ("cp315-abi3-manylinux_2_28_x86_64", "x86_64", True),
        ("cp314-cp314-manylinux_2_28_x86_64", "x86_64", False),
        ("cp316-abi3-manylinux_2_28_x86_64", "x86_64", False),
        ("cp315-cp315t-manylinux_2_28_x86_64", "x86_64", False),
        ("cp315t-cp315t-manylinux_2_28_x86_64", "x86_64", False),
        ("cp315-cp315-manylinux_2_28_x86_64", "aarch64", False),
        ("cp315-cp315-win_amd64", "x86_64", False),
        ("cp315-cp315-linux_x86_64", "x86_64", False),
        ("py2-none-any", "x86_64", False),
    ],
)
def test_target_wheel_tags(tags, architecture, expected):
    assert wheels.is_wheel_compatible(f"example-1.0-{tags}.whl", "3.15.0", architecture) is expected


def test_wheel_tags_follow_requested_python_version():
    assert wheels.is_wheel_compatible("example-1.0-cp314-cp314-any.whl", "3.14.0", "x86_64")
    assert not wheels.is_wheel_compatible("not-a-wheel.tar.gz", "3.15.0", "x86_64")


def test_read_exact_and_url_pins(tmp_path):
    path = tmp_path / "pins.txt"
    path.write_text("# pins\n\nSome_Package[extra]==1.0\nother @ https://example.com/file.whl\n")
    assert wheels.read_requirements(path) == {
        "some-package": ("1.0", None),
        "other": ("", "https://example.com/file.whl"),
    }


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("# no pins", "No third-party pins"),
        ("example>=1", "Expected an exact"),
        ("example==1.*", "Expected an exact"),
        ("example==1; python_version > '3.14'", "Apply target markers"),
        ("example==1\nexample==2", "Conflicting pins"),
    ],
)
def test_reject_unresolved_or_conflicting_inputs(tmp_path, text, message):
    path = tmp_path / "pins.txt"
    path.write_text(text)
    with pytest.raises(ValueError, match=message):
        wheels.read_requirements(path)


@pytest.mark.parametrize(
    ("names", "fallback", "disposition"),
    [
        (["pkg/module.py"], False, "pure-python-sdist"),
        (["pkg/module.c", "pkg/module.py"], False, "compiled-sources-review"),
        (["pkg/Cargo.toml", "pkg/src/lib.rs"], False, "compiled-sources-review"),
        (["pkg/CMakeLists.txt"], False, "compiled-sources-review"),
        (["pkg/module.pyx"], True, "pure-python-fallback"),
    ],
)
@pytest.mark.parametrize("archive_type", ["zip", "tar.gz"])
def test_sdist_classification(names, fallback, disposition, archive_type):
    payload = BytesIO()
    if archive_type == "zip":
        with zipfile.ZipFile(payload, "w") as archive:
            for name in names:
                archive.writestr(name, "")
    else:
        with tarfile.open(fileobj=payload, mode="w:gz") as archive:
            for name in names:
                archive.addfile(tarfile.TarInfo(name))
    result = wheels.classify_sdist(payload.getvalue(), f"pkg.{archive_type}", pure_python_fallback=fallback)
    assert result["disposition"] == disposition
    assert bool(result["compiled_source_evidence"]) is (disposition != "pure-python-sdist")


def build_release(version, tags="py3-none-any", requires_python="", yanked=False):
    filename = f"example-{version}-{tags}.whl"
    return {
        "info": {"version": version, "requires_python": requires_python},
        "urls": [
            {
                "filename": filename,
                "url": f"https://example.com/{filename}",
                "packagetype": "bdist_wheel",
                "yanked": yanked,
            }
        ],
    }


@pytest.mark.parametrize(
    ("requires_python", "yanked", "status"),
    [("", False, "wheel"), ("<3.15", False, "unavailable"), ("", True, "unavailable")],
)
def test_release_metadata_and_yanked_files(requires_python, yanked, status):
    result = wheels.audit_release(
        build_release("1.0", requires_python=requires_python, yanked=yanked),
        "3.15.0",
        "x86_64",
        pure_python_fallback=False,
    )
    assert result["status"] == status


@mock.patch.object(wheels, "fetch_bytes", autospec=True)
def test_source_release_records_confirmed_fallback(fetch_bytes):
    payload = BytesIO()
    with zipfile.ZipFile(payload, "w") as archive:
        archive.writestr("example/extension.c", "")
    fetch_bytes.return_value = payload.getvalue()
    data = {
        "info": {"version": "1"},
        "urls": [
            {"filename": "example.zip", "url": "https://example.com/example.zip", "packagetype": "sdist"}
        ],
    }
    result = wheels.audit_release(data, "3.15.0", "x86_64", pure_python_fallback=True)
    assert result["status"] == "source-only"
    assert result["disposition"] == "pure-python-fallback"


@mock.patch.object(wheels, "fetch_release", autospec=True)
def test_selected_pin_and_newest_release_are_separate(fetch_release):
    fetch_release.side_effect = [build_release("1", "cp314-cp314-any"), build_release("2")]
    result = wheels.audit_requirement("example", ("1", None), "amd64", "3.15.0", set())
    assert result["selected"]["status"] == "unavailable"
    assert result["latest"]["status"] == "wheel"
    assert result["latest"]["version"] == "2"
    assert fetch_release.call_args_list == [mock.call("example", "1"), mock.call("example")]


@pytest.mark.parametrize(
    ("url", "status"),
    [
        ("https://example.com/example-1-py3-none-any.whl?download=1#hash", "wheel-url"),
        ("git+https://example.com/repo", "source-url-review"),
    ],
)
@mock.patch.object(wheels, "fetch_release", autospec=True)
def test_url_pins_do_not_claim_registry_wheel_evidence(fetch_release, url, status):
    fetch_release.return_value = build_release("2")
    result = wheels.audit_requirement("example", ("", url), "arm64", "3.15.0", set())
    assert result["selected"]["status"] == status
    assert result["latest"]["status"] == "wheel"


@mock.patch.object(wheels, "fetch_release", autospec=True, side_effect=URLError("unreachable"))
def test_network_failure_remains_unverified(fetch_release):
    result = wheels.audit_requirement("example", ("1", None), "amd64", "3.15.0", set())
    assert "lookup_error" in result
    assert "latest" not in result


@pytest.mark.parametrize(("failed", "exit_code"), [(False, 0), (True, 1)])
@mock.patch.object(wheels, "audit_requirement", autospec=True)
def test_main_preserves_platform_specific_pins_and_reports_errors(
    audit_requirement, tmp_path, failed, exit_code
):
    amd64, arm64, output = (tmp_path / name for name in ("amd64.txt", "arm64.txt", "audit.json"))
    amd64.write_text("example==1\n")
    arm64.write_text("example==2\n")
    audit_requirement.side_effect = lambda name, pin, platform, *args: {
        "name": name,
        "pin": pin[0],
        "platform": platform,
        **({"lookup_error": "failed"} if failed else {}),
    }
    assert (
        wheels.main(
            [
                "--python-version",
                "3.15.0",
                "--amd64",
                str(amd64),
                "--arm64",
                str(arm64),
                "--output",
                str(output),
            ]
        )
        == exit_code
    )
    report = json.loads(output.read_text())
    assert report["python"] == "3.15.0"
    assert [(record["platform"], record["pin"]) for record in report["packages"]] == [
        ("amd64", "1"),
        ("arm64", "2"),
    ]


def test_main_requires_final_target_version(tmp_path):
    with pytest.raises(SystemExit, match="2"):
        wheels.main(
            [
                "--python-version",
                "3.15rc3",
                "--amd64",
                "unused",
                "--arm64",
                "unused",
                "--output",
                str(tmp_path / "unused"),
            ]
        )
