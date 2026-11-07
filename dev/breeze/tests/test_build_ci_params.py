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

import pytest

from airflow_breeze.params.build_ci_params import BuildCiParams

INLINE_CACHE_FLAG = "--build-arg=BUILDKIT_INLINE_CACHE=1"


@pytest.mark.parametrize(
    ("docker_cache", "inline_cache_expected"),
    [
        pytest.param("registry", True, id="registry"),
        pytest.param("local", True, id="local"),
        pytest.param("disabled", False, id="disabled"),
    ],
)
def test_inline_cache_recorded_unless_cache_disabled(docker_cache: str, inline_cache_expected: bool):
    flags = BuildCiParams(docker_cache=docker_cache).common_docker_build_flags
    assert (INLINE_CACHE_FLAG in flags) is inline_cache_expected


def test_cache_from_image_added_next_to_registry_cache():
    params = BuildCiParams(docker_cache="registry", cache_from_image="localhost:5000/ci-image-cache:3.12")
    flags = params.common_docker_build_flags
    assert f"--cache-from={params.get_cache(params.platform)}" in flags
    assert "--cache-from=localhost:5000/ci-image-cache:3.12" in flags


def test_cache_from_image_added_when_registry_cache_is_not_used():
    flags = BuildCiParams(
        docker_cache="disabled", cache_from_image="localhost:5000/ci-image-cache:3.12"
    ).common_docker_build_flags
    assert "--cache-from=localhost:5000/ci-image-cache:3.12" in flags
    assert "--no-cache" in flags


def test_no_cache_from_image_flag_when_unset():
    flags = BuildCiParams(docker_cache="registry").common_docker_build_flags
    assert not any(flag.startswith("--cache-from=localhost") for flag in flags)
