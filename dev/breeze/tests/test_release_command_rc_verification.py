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

from pathlib import Path

import click
import pytest

from airflow_breeze.commands.release_command import verify_is_latest_release_candidate_or_raise


def _touch_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def test_verify_latest_airflow_rc_passes_for_latest(tmp_path: Path) -> None:
    # Arrange
    svn_dev_airflow = tmp_path / "asf-dist" / "dev" / "airflow"
    _touch_dir(svn_dev_airflow / "3.1.4rc1")
    _touch_dir(svn_dev_airflow / "3.1.4rc2")

    # Act / Assert
    verify_is_latest_release_candidate_or_raise(
        release_candidate="3.1.4rc2",
        svn_dev_repo=str(svn_dev_airflow),
        option_label="Airflow release candidate",
        option_name="--release-candidate",
    )


def test_verify_latest_airflow_rc_fails_for_non_latest(tmp_path: Path) -> None:
    # Arrange
    svn_dev_airflow = tmp_path / "asf-dist" / "dev" / "airflow"
    _touch_dir(svn_dev_airflow / "3.1.4rc1")
    _touch_dir(svn_dev_airflow / "3.1.4rc2")

    # Act / Assert
    with pytest.raises(click.ClickException, match=r"not the latest RC"):
        verify_is_latest_release_candidate_or_raise(
            release_candidate="3.1.4rc1",
            svn_dev_repo=str(svn_dev_airflow),
            option_label="Airflow release candidate",
            option_name="--release-candidate",
        )


def test_verify_latest_task_sdk_rc_uses_task_sdk_subdir(tmp_path: Path) -> None:
    # Arrange
    svn_dev_airflow = tmp_path / "asf-dist" / "dev" / "airflow"
    task_sdk_dir = svn_dev_airflow / "task-sdk"
    _touch_dir(task_sdk_dir / "1.2.3rc1")
    _touch_dir(task_sdk_dir / "1.2.3rc2")

    # Act / Assert
    verify_is_latest_release_candidate_or_raise(
        release_candidate="1.2.3rc2",
        svn_dev_repo=str(svn_dev_airflow),
        svn_subdir="task-sdk",
        option_label="Task SDK release candidate",
        option_name="--task-sdk-release-candidate",
    )


@pytest.mark.parametrize("bad_version", ["3.1.4", "3.1.4a1", "not-a-version", "3.1"])
def test_verify_latest_rejects_non_rc_versions(tmp_path: Path, bad_version: str) -> None:
    # Arrange
    svn_dev_airflow = tmp_path / "asf-dist" / "dev" / "airflow"
    _touch_dir(svn_dev_airflow)

    # Act / Assert
    with pytest.raises(click.ClickException, match=r"must be an RC version|Invalid"):
        verify_is_latest_release_candidate_or_raise(
            release_candidate=bad_version,
            svn_dev_repo=str(svn_dev_airflow),
            option_label="Airflow release candidate",
            option_name="--release-candidate",
        )


def test_verify_latest_handles_rc10_correctly(tmp_path: Path) -> None:
    # Arrange
    svn_dev_airflow = tmp_path / "asf-dist" / "dev" / "airflow"
    _touch_dir(svn_dev_airflow / "3.1.4rc9")
    _touch_dir(svn_dev_airflow / "3.1.4rc10")

    # Act / Assert
    verify_is_latest_release_candidate_or_raise(
        release_candidate="3.1.4rc10",
        svn_dev_repo=str(svn_dev_airflow),
        option_label="Airflow release candidate",
        option_name="--release-candidate",
    )

