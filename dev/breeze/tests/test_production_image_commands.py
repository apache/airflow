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
from unittest import mock

import pytest
import yaml

from airflow_breeze.commands.production_image_commands import load
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH


def get_stash_key_of_workflow() -> str:
    """The key that prod-image-build.yml stashes the image under, for linux/amd64 and Python 3.11."""
    workflow = yaml.safe_load((AIRFLOW_ROOT_PATH / ".github/workflows/prod-image-build.yml").read_text())
    keys = [
        step["with"]["key"]
        for step in workflow["jobs"]["build-prod-images"]["steps"]
        if step.get("uses", "").endswith("/stash-save")
    ]
    assert len(keys) == 1
    key = keys[0].replace("${{ inputs.platform }}", "linux_amd64")
    return key.replace("${{ env.PYTHON_MAJOR_MINOR_VERSION }}", "3.11")


@pytest.mark.parametrize("image_file", [None, Path("prod-image-save-linux_amd64-custom-3.11.tar")])
@pytest.mark.parametrize("source", ["from_run", "from_pr"])
@mock.patch("airflow_breeze.commands.production_image_commands.download_artifact_from_pr", autospec=True)
@mock.patch("airflow_breeze.commands.production_image_commands.download_artifact_from_run_id", autospec=True)
@mock.patch("airflow_breeze.commands.production_image_commands.run_command", autospec=True)
@mock.patch("airflow_breeze.commands.production_image_commands.perform_environment_checks", autospec=True)
def test_load_downloads_the_artifact_stashed_by_the_workflow(
    mock_perform_environment_checks,
    mock_run_command,
    mock_download_from_run,
    mock_download_from_pr,
    source,
    image_file,
    tmp_path,
):
    mock_download = {"from_run": mock_download_from_run, "from_pr": mock_download_from_pr}[source]
    mock_download.side_effect = lambda _id, _prefix, output_file, *_: output_file.touch()
    mock_run_command.return_value = mock.MagicMock(returncode=0)
    expected_file = tmp_path / (image_file or "prod-image-save-linux_amd64-3.11.tar")

    load.callback(
        **{"from_run": "", "from_pr": "", source: "123"},
        github_repository="apache/airflow",
        github_token="token",
        image_file=image_file,
        image_file_dir=tmp_path,
        platform="linux/amd64",
        python="3.11",
        skip_image_file_deletion=False,
    )

    mock_download.assert_called_once_with(
        "123", get_stash_key_of_workflow(), expected_file, "apache/airflow", "token"
    )
    assert ["docker", "image", "load", "-i", expected_file.as_posix()] in [
        call.args[0] for call in mock_run_command.call_args_list
    ]
