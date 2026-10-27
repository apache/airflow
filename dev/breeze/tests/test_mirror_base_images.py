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

from subprocess import CompletedProcess
from unittest import mock

import pytest
import requests
from click.testing import CliRunner

from airflow_breeze.commands.release_management_commands import (
    get_latest_hardened_python_patchlevel,
    mirror_base_images,
)
from airflow_breeze.global_constants import ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION


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
    with (
        mock.patch(
            "airflow_breeze.commands.release_management_commands.get_latest_hardened_python_patchlevel",
            autospec=True,
            return_value=latest,
        ),
        mock.patch(
            "airflow_breeze.commands.release_management_commands.run_command",
            autospec=True,
            return_value=CompletedProcess(args=[], returncode=0),
        ) as run,
    ):
        result = CliRunner().invoke(mirror_base_images, ["--python", "3.14"], catch_exceptions=False)

    assert result.exit_code == 0
    sources = [call.args[0][-1] for call in run.call_args_list]
    if newer_available:
        assert sources == [
            f"dhi.io/python:{pinned}-debian13-dev",
            "dhi.io/python:3.14.99-debian13-dev",
            f"dhi.io/python:{pinned}-debian12-dev",
            "dhi.io/python:3.14.99-debian12-dev",
        ]
        assert "ghcr.io/apache/airflow/base/python:3.14.99-debian13-dev" in run.call_args_list[1].args[0]
    else:
        assert sources == [f"dhi.io/python:{pinned}-debian13-dev", f"dhi.io/python:{pinned}-debian12-dev"]
