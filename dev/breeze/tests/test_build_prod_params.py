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
