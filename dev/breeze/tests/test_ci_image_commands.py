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
from unittest import mock

import pytest

from airflow_breeze.commands.ci_image_commands import (
    build_ci_image_if_needed,
    confirm_build_if_sources_changed,
    get_ci_image_sources_hash_label,
    is_ci_image_built_from_current_sources,
)
from airflow_breeze.global_constants import CI_IMAGE_SOURCES_HASH_LABEL
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.utils.md5_build_check import calculate_ci_sources_hash

CI_IMAGE = "ghcr.io/apache/airflow/main/ci/python3.10"


def test_calculate_ci_sources_hash_is_stable_across_checkouts(tmp_path, monkeypatch):
    watched_files = ["Dockerfile.ci", "scripts/docker/common.sh"]
    for checkout in ("worktree-a", "worktree-b"):
        root = tmp_path / checkout
        (root / "scripts" / "docker").mkdir(parents=True)
        (root / "Dockerfile.ci").write_text("FROM base")
        (root / "scripts" / "docker" / "common.sh").write_text("echo common")
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.FILES_FOR_REBUILD_CHECK", watched_files)
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.AIRFLOW_ROOT_PATH", tmp_path / "worktree-a")
    hash_of_first_checkout = calculate_ci_sources_hash()
    monkeypatch.setattr("airflow_breeze.utils.md5_build_check.AIRFLOW_ROOT_PATH", tmp_path / "worktree-b")
    assert calculate_ci_sources_hash() == hash_of_first_checkout
    (tmp_path / "worktree-b" / "Dockerfile.ci").write_text("FROM other")
    assert calculate_ci_sources_hash() != hash_of_first_checkout


@pytest.mark.parametrize(
    ("returncode", "stdout", "expected"),
    [
        pytest.param(0, json.dumps({CI_IMAGE_SOURCES_HASH_LABEL: "abc"}), "abc", id="label-present"),
        pytest.param(0, json.dumps({"other-label": "abc"}), None, id="label-absent"),
        pytest.param(0, "null", None, id="no-labels-at-all"),
        pytest.param(0, "", None, id="empty-output"),
        pytest.param(0, "not-json", None, id="invalid-json"),
        pytest.param(1, "", None, id="image-missing"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_image_commands.run_command")
def test_get_ci_image_sources_hash_label(mock_run_command, returncode, stdout, expected):
    mock_run_command.return_value = mock.MagicMock(returncode=returncode, stdout=stdout)
    assert get_ci_image_sources_hash_label(CI_IMAGE) == expected


@pytest.mark.parametrize(
    ("image_hash", "current_hash", "expected"),
    [
        pytest.param("abc", "abc", True, id="match"),
        pytest.param("abc", "def", False, id="mismatch"),
        pytest.param(None, "abc", False, id="no-label"),
    ],
)
@mock.patch("airflow_breeze.commands.ci_image_commands.calculate_ci_sources_hash")
@mock.patch("airflow_breeze.commands.ci_image_commands.get_ci_image_sources_hash_label")
def test_is_ci_image_built_from_current_sources(
    mock_get_ci_image_sources_hash_label,
    mock_calculate_ci_sources_hash,
    image_hash,
    current_hash,
    expected,
):
    mock_get_ci_image_sources_hash_label.return_value = image_hash
    mock_calculate_ci_sources_hash.return_value = current_hash
    assert is_ci_image_built_from_current_sources(BuildCiParams()) is expected


@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_confirm_build_if_sources_changed_skips_build_when_image_matches_current_sources(
    mock_is_ci_image_built_from_current_sources, mock_mark_image_as_rebuilt
):
    mock_is_ci_image_built_from_current_sources.return_value = True
    build_ci_params = BuildCiParams()
    assert confirm_build_if_sources_changed(build_ci_params) is False
    mock_mark_image_as_rebuilt.assert_called_once_with(ci_image_params=build_ci_params)


@mock.patch("airflow_breeze.commands.ci_image_commands.md5sum_check_if_build_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_confirm_build_if_sources_changed_falls_back_to_md5_check_when_image_does_not_match(
    mock_is_ci_image_built_from_current_sources,
    mock_mark_image_as_rebuilt,
    mock_md5sum_check_if_build_is_needed,
):
    mock_is_ci_image_built_from_current_sources.return_value = False
    mock_md5sum_check_if_build_is_needed.return_value = False
    assert confirm_build_if_sources_changed(BuildCiParams()) is False
    mock_mark_image_as_rebuilt.assert_not_called()
    mock_md5sum_check_if_build_is_needed.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.mark_image_as_rebuilt")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_reuses_image_built_in_another_checkout(
    mock_is_ci_image_built_from_current_sources,
    mock_mark_image_as_rebuilt,
    mock_run_build_ci_image,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_is_ci_image_built_from_current_sources.return_value = True
    build_ci_image_if_needed(command_params=BuildCiParams())
    mock_mark_image_as_rebuilt.assert_called_once()
    mock_run_build_ci_image.assert_not_called()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_forces_build_when_image_does_not_match_sources(
    mock_is_ci_image_built_from_current_sources,
    mock_run_build_ci_image,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_is_ci_image_built_from_current_sources.return_value = False
    mock_check_if_image_building_is_needed.return_value = True
    mock_run_build_ci_image.return_value = (0, "built")
    build_ci_image_if_needed(command_params=BuildCiParams())
    assert mock_check_if_image_building_is_needed.call_args.kwargs["ci_image_params"].force_build is True
    mock_run_build_ci_image.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.run_build_ci_image")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_does_not_reuse_image_when_force_build_requested(
    mock_is_ci_image_built_from_current_sources,
    mock_run_build_ci_image,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    mock_check_if_image_building_is_needed.return_value = True
    mock_run_build_ci_image.return_value = (0, "built")
    build_ci_image_if_needed(command_params=BuildCiParams(force_build=True))
    mock_is_ci_image_built_from_current_sources.assert_not_called()
    mock_run_build_ci_image.assert_called_once()


@mock.patch("airflow_breeze.commands.ci_image_commands.check_if_image_building_is_needed")
@mock.patch("airflow_breeze.commands.ci_image_commands.is_ci_image_built_from_current_sources")
def test_build_ci_image_if_needed_does_not_query_docker_when_marker_present(
    mock_is_ci_image_built_from_current_sources,
    mock_check_if_image_building_is_needed,
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr("airflow_breeze.commands.ci_image_commands.BUILD_CACHE_PATH", tmp_path)
    command_params = BuildCiParams()
    marker = tmp_path / command_params.airflow_branch / f".built_{command_params.python}"
    marker.parent.mkdir(parents=True)
    marker.touch()
    mock_check_if_image_building_is_needed.return_value = False
    build_ci_image_if_needed(command_params=command_params)
    mock_is_ci_image_built_from_current_sources.assert_not_called()
