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

from unittest import mock

import pytest

from airflow_breeze.global_constants import (
    ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION,
    HARDENED_PYTHON_IMAGE_DIGESTS,
    LEGACY_PYTHON_VERSIONS,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams

PYTHON_3_12_TRIXIE_TAG = f"{ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION['3.12']}-debian13-dev"
PYTHON_3_12_BOOKWORM_TAG = f"{ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION['3.12']}-debian12-dev"


def _build_args(params) -> dict[str, str]:
    values = params.prepare_arguments_for_docker_build_command()
    return dict(value.split("=", 1) for value in values[1::2])


@pytest.mark.parametrize(
    ("image_flavor", "debian_version", "expected_base_image"),
    [
        pytest.param(
            "hardened",
            "trixie",
            f"ghcr.io/apache/airflow/base/python:{PYTHON_3_12_TRIXIE_TAG}"
            f"@{HARDENED_PYTHON_IMAGE_DIGESTS[PYTHON_3_12_TRIXIE_TAG]}",
            id="hardened-trixie",
        ),
        pytest.param(
            "hardened",
            "bookworm",
            f"ghcr.io/apache/airflow/base/python:{PYTHON_3_12_BOOKWORM_TAG}"
            f"@{HARDENED_PYTHON_IMAGE_DIGESTS[PYTHON_3_12_BOOKWORM_TAG]}",
            id="hardened-bookworm",
        ),
        pytest.param("legacy", "trixie", "debian:trixie-slim", id="legacy-trixie"),
        pytest.param("legacy", "bookworm", "debian:bookworm-slim", id="legacy-bookworm"),
    ],
)
def test_prod_image_base_image_follows_flavor_and_debian_version(
    image_flavor, debian_version, expected_base_image
):
    params = BuildProdParams(python="3.12", image_flavor=image_flavor, debian_version=debian_version)

    build_args = _build_args(params)

    assert build_args["BASE_IMAGE"] == expected_base_image
    assert build_args["AIRFLOW_IMAGE_FLAVOR"] == image_flavor


@pytest.mark.parametrize(
    ("image_flavor", "expected_python_version"),
    [
        pytest.param("hardened", ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION["3.12"], id="hardened"),
        pytest.param("legacy", "3.12.99", id="legacy"),
    ],
)
@mock.patch.dict(LEGACY_PYTHON_VERSIONS, {"3.12": "3.12.99"})
def test_prod_image_python_version_follows_flavor(image_flavor, expected_python_version):
    build_args = _build_args(BuildProdParams(python="3.12", image_flavor=image_flavor))

    assert build_args["AIRFLOW_PYTHON_VERSION"] == expected_python_version


@pytest.mark.parametrize("image_flavor", ["hardened", "legacy"])
def test_explicit_python_image_wins_over_flavor(image_flavor):
    params = BuildProdParams(python="3.12", image_flavor=image_flavor, python_image="my-registry/python:x")

    assert _build_args(params)["BASE_IMAGE"] == "my-registry/python:x"


def test_prod_image_defaults_to_hardened_trixie():
    build_args = _build_args(BuildProdParams(python="3.12"))

    assert build_args["AIRFLOW_IMAGE_FLAVOR"] == "hardened"
    assert build_args["BASE_IMAGE"].split("@")[0].endswith("-debian13-dev")


def test_ci_image_is_hardened_trixie():
    build_args = _build_args(BuildCiParams(python="3.12"))

    assert build_args["BASE_IMAGE"] == (
        f"ghcr.io/apache/airflow/base/python:{PYTHON_3_12_TRIXIE_TAG}"
        f"@{HARDENED_PYTHON_IMAGE_DIGESTS[PYTHON_3_12_TRIXIE_TAG]}"
    )
    assert "AIRFLOW_IMAGE_FLAVOR" not in build_args


def test_base_image_without_a_pinned_digest_uses_the_tag():
    # A Python version Airflow does not pin - for example one passed in to try a new release.
    build_args = _build_args(BuildCiParams(python="3.99"))

    assert build_args["BASE_IMAGE"] == "ghcr.io/apache/airflow/base/python:3.99-debian13-dev"


WITHOUT_HARDENED_IMAGES = "airflow_breeze.params.common_build_params.PYTHON_VERSIONS_WITHOUT_HARDENED_IMAGES"


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(BuildCiParams(python="3.12"), id="ci"),
        pytest.param(BuildProdParams(python="3.12", image_flavor="hardened"), id="prod"),
        pytest.param(BuildCiParams(python="3.15"), id="ci-new-python"),
        pytest.param(BuildProdParams(python="3.15"), id="prod-new-python"),
    ],
)
@mock.patch(WITHOUT_HARDENED_IMAGES, ["3.12", "3.15"])
def test_image_falls_back_to_legacy_without_hardened_image(params):
    build_args = _build_args(params)

    assert params.resolved_image_flavor == "legacy"
    assert build_args["BASE_IMAGE"] == "debian:trixie-slim"
    assert build_args["AIRFLOW_IMAGE_FLAVOR"] == "legacy"
    assert build_args["AIRFLOW_PYTHON_VERSION"] == LEGACY_PYTHON_VERSIONS.get(
        params.python, ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION[params.python]
    )
    assert "CI_OS_DEPS_IMAGE" not in build_args
    assert "DEV_OS_DEPS_IMAGE" not in build_args
    assert "RUNTIME_OS_DEPS_IMAGE" not in build_args


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(BuildCiParams(python="3.12", python_image="my-registry/python:x"), id="ci"),
        pytest.param(BuildProdParams(python="3.12", python_image="my-registry/python:x"), id="prod"),
    ],
)
@mock.patch(WITHOUT_HARDENED_IMAGES, ["3.12"])
def test_explicit_python_image_disables_the_legacy_fallback(params):
    assert params.resolved_image_flavor == "hardened"
    assert _build_args(params)["BASE_IMAGE"] == "my-registry/python:x"


@pytest.mark.parametrize(
    ("params", "expected_prefix"),
    [
        pytest.param(
            BuildCiParams(python="3.12", github_actions="true"),
            "::warning title=Legacy CI image for Python 3.12::No hardened",
            id="ci-github-actions",
        ),
        pytest.param(
            BuildProdParams(python="3.12", github_actions="true"),
            "::warning title=Legacy PROD image for Python 3.12::No hardened",
            id="prod-github-actions",
        ),
        pytest.param(
            BuildCiParams(python="3.12", github_actions="false"), "[warning]No hardened", id="local"
        ),
    ],
)
@mock.patch(WITHOUT_HARDENED_IMAGES, ["3.12"])
@mock.patch("airflow_breeze.params.common_build_params.get_console", autospec=True)
def test_warning_when_hardened_image_unavailable(mock_get_console, params, expected_prefix):
    params.warn_if_hardened_image_unavailable()

    printed = mock_get_console.return_value.print.call_args.args[0]
    assert printed.startswith(expected_prefix)
    assert "debian:trixie-slim" in printed
    assert "\n" not in printed


@pytest.mark.parametrize(
    "params",
    [
        pytest.param(BuildCiParams(python="3.12"), id="hardened-image-available"),
        pytest.param(BuildProdParams(python="3.13", image_flavor="legacy"), id="legacy-chosen-explicitly"),
    ],
)
@mock.patch(WITHOUT_HARDENED_IMAGES, ["3.15"])
@mock.patch("airflow_breeze.params.common_build_params.get_console", autospec=True)
def test_no_warning_when_hardened_image_is_available(mock_get_console, params):
    params.warn_if_hardened_image_unavailable()

    mock_get_console.assert_not_called()
