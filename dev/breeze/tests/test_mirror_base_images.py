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
from subprocess import CompletedProcess
from unittest import mock

import pytest
import requests
from click.testing import CliRunner

from airflow_breeze.commands.release_management_commands import (
    get_latest_hardened_python_patchlevel,
    mirror_base_images,
)
from airflow_breeze.global_constants import (
    ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION,
    HARDENED_PYTHON_IMAGE_DIGESTS,
)

MIRROR = "ghcr.io/apache/airflow/base/python"
PINNED_TAG = f"{ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION['3.14']}-debian13-dev"
PINNED_DIGEST = HARDENED_PYTHON_IMAGE_DIGESTS[PINNED_TAG]
NEW_DIGEST = "sha256:" + "ab" * 32


class FakeRegistry:
    """Answers run_command for imagetools, moving each mirrored tag to the digest given for it."""

    def __init__(self, digests_after_copy: dict[str, str]):
        self.digests: dict[str, str] = {}
        self.digests_after_copy = digests_after_copy
        self.copies: list[tuple[str, list[str]]] = []

    def __call__(self, cmd, **kwargs):
        if cmd[3] == "inspect":
            digest = self.digests.get(cmd[4])
            stdout = json.dumps({"digest": digest}) if digest else ""
            return CompletedProcess(args=cmd, returncode=0 if digest else 1, stdout=stdout)
        targets = [cmd[i + 1] for i, arg in enumerate(cmd) if arg == "--tag"]
        self.copies.append((cmd[-1], targets))
        for target in targets:
            tag = target.split(":", 1)[1]
            if tag in self.digests_after_copy:
                self.digests[target] = self.digests_after_copy[tag]
        return CompletedProcess(args=cmd, returncode=0)

    @property
    def sources(self) -> list[str]:
        return [source for source, _ in self.copies if source.partition("/")[0] == "dhi.io"]


def _catalog_response(text: str) -> mock.MagicMock:
    response = mock.MagicMock(spec=requests.Response)
    response.text = text
    return response


@pytest.mark.parametrize(
    ("definition", "expected"),
    [
        pytest.param(
            "tags:\n  - 3.14-debian12-dev\n  - 3.14.8-debian12-dev\n", "3.14.8", id="patchlevel-tag"
        ),
        pytest.param(
            "tags:\n  - 3.12.9-debian12-dev\n  - 3.12.15-debian12-dev\n", "3.12.15", id="numeric-max"
        ),
        pytest.param("tags:\n  - 3.14-debian12-dev\n", None, id="no-patchlevel-tag"),
    ],
)
def test_latest_hardened_python_patchlevel_is_read_from_the_catalog(definition, expected):
    with mock.patch("requests.get", autospec=True, return_value=_catalog_response(definition)) as get:
        assert get_latest_hardened_python_patchlevel("3.14", "bookworm") == expected
    assert get.call_args.args[0].endswith("/image/python/debian-12/3.14-dev.yaml")


def test_latest_hardened_python_patchlevel_defaults_to_trixie():
    definition = "tags:\n  - 3.14.8-debian12-dev\n  - 3.14.9-debian13-dev\n"
    with mock.patch("requests.get", autospec=True, return_value=_catalog_response(definition)) as get:
        assert get_latest_hardened_python_patchlevel("3.14") == "3.14.9"
    assert get.call_args.args[0].endswith("/image/python/debian-13/3.14-dev.yaml")


def test_latest_hardened_python_patchlevel_is_none_when_the_catalog_is_unreachable():
    with mock.patch("requests.get", autospec=True, side_effect=requests.ConnectionError("down")):
        assert get_latest_hardened_python_patchlevel("3.14") is None


@pytest.mark.parametrize("newer_available", [True, False])
def test_mirror_also_copies_a_newer_published_patchlevel(newer_available):
    pinned = ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION["3.14"]
    latest = "3.14.99" if newer_available else pinned
    registry = FakeRegistry({})
    with (
        mock.patch(
            "airflow_breeze.commands.release_management_commands.get_latest_hardened_python_patchlevel",
            autospec=True,
            return_value=latest,
        ),
        mock.patch(
            "airflow_breeze.commands.release_management_commands.run_command",
            autospec=True,
            side_effect=registry,
        ),
    ):
        result = CliRunner().invoke(
            mirror_base_images, ["--python", "3.14"], env={"GITHUB_OUTPUT": None}, catch_exceptions=False
        )

    assert result.exit_code == 0
    if newer_available:
        assert registry.sources == [
            f"dhi.io/python:{pinned}-debian13-dev",
            "dhi.io/python:3.14.99-debian13-dev",
            f"dhi.io/python:{pinned}-debian12-dev",
            "dhi.io/python:3.14.99-debian12-dev",
        ]
        assert (
            "dhi.io/python:3.14.99-debian13-dev",
            [f"{MIRROR}:3.14.99-debian13-dev"],
        ) in registry.copies
    else:
        assert registry.sources == [
            f"dhi.io/python:{pinned}-debian13-dev",
            f"dhi.io/python:{pinned}-debian12-dev",
        ]


@mock.patch(
    "airflow_breeze.commands.release_management_commands.get_latest_hardened_python_patchlevel",
    autospec=True,
    return_value=None,
)
def test_mirror_keeps_the_pinned_and_the_mirrored_digests_tagged(mock_latest):
    registry = FakeRegistry({PINNED_TAG: NEW_DIGEST})
    with mock.patch(
        "airflow_breeze.commands.release_management_commands.run_command",
        autospec=True,
        side_effect=registry,
    ):
        result = CliRunner().invoke(
            mirror_base_images, ["--python", "3.14"], env={"GITHUB_OUTPUT": None}, catch_exceptions=False
        )

    assert result.exit_code == 0
    assert (f"{MIRROR}:{PINNED_TAG}@{NEW_DIGEST}", [f"{MIRROR}:{PINNED_TAG}-{'ab' * 6}"]) in registry.copies
    assert (
        f"{MIRROR}:{PINNED_TAG}@{PINNED_DIGEST}",
        [f"{MIRROR}:{PINNED_TAG}-{PINNED_DIGEST.removeprefix('sha256:')[:12]}"],
    ) in registry.copies


@pytest.mark.parametrize(
    ("previous_digest", "new_digest", "expected"),
    [
        pytest.param(PINNED_DIGEST, NEW_DIGEST, "true", id="moved-past-the-pin"),
        pytest.param(NEW_DIGEST, NEW_DIGEST, "false", id="already-behind-not-moved"),
        pytest.param(NEW_DIGEST, PINNED_DIGEST, "false", id="moved-back-to-the-pin"),
    ],
)
@mock.patch(
    "airflow_breeze.commands.release_management_commands.get_latest_hardened_python_patchlevel",
    autospec=True,
    return_value=None,
)
def test_mirror_reports_moving_past_the_pins(mock_latest, previous_digest, new_digest, expected, tmp_path):
    registry = FakeRegistry({PINNED_TAG: new_digest})
    registry.digests[f"{MIRROR}:{PINNED_TAG}"] = previous_digest
    github_output = tmp_path / "github_output"
    with mock.patch(
        "airflow_breeze.commands.release_management_commands.run_command",
        autospec=True,
        side_effect=registry,
    ):
        result = CliRunner().invoke(
            mirror_base_images,
            ["--python", "3.14"],
            env={"GITHUB_OUTPUT": str(github_output)},
            catch_exceptions=False,
        )

    assert result.exit_code == 0
    assert github_output.read_text() == f"moved-past-pins={expected}\n"
