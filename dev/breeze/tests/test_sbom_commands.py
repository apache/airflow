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
from unittest.mock import patch

import pytest

from airflow_breeze.commands.sbom_commands import update_sbom_information
from airflow_breeze.utils.cdxgen import SbomCoreJob
from airflow_breeze.utils.path_utils import FILES_SBOM_PATH


@pytest.mark.parametrize(
    ("constraints_reference", "expected_constraints_reference"),
    [
        ("constraints-main", "constraints-main"),
        (None, "constraints-3.2.0"),
    ],
)
def test_sbom_core_job_resolves_constraints_reference(
    tmp_path, constraints_reference, expected_constraints_reference
):
    job = SbomCoreJob(
        python_version="3.10",
        target_path=tmp_path / "output.json",
        airflow_version="3.2.0",
        application_root_path=FILES_SBOM_PATH,
        include_provider_dependencies=False,
        include_python=True,
        include_npm=False,
        constraints_reference=constraints_reference,
    )
    with patch(
        "airflow_breeze.utils.cdxgen.download_constraints_file", autospec=True, return_value=True
    ) as mock_dl:
        job.download_dependency_files(output=None, github_token=None)

    mock_dl.assert_called_once()
    assert mock_dl.call_args.kwargs["constraints_reference"] == expected_constraints_reference


def test_update_sbom_information_with_airflow_root_path_skips_released_versions_lookup(tmp_path):
    airflow_root_path = Path(tmp_path)
    airflow_docs_dir = airflow_root_path / "generated" / "_build" / "docs" / "apache-airflow" / "stable"
    airflow_docs_dir.mkdir(parents=True)

    with (
        patch("airflow_breeze.utils.cdxgen.start_cdxgen_servers", autospec=True),
        patch(
            "airflow_breeze.utils.github.get_active_airflow_versions",
            autospec=True,
            return_value=(["3.2.0"], {}),
        ) as mock_get_active_airflow_versions,
        patch("airflow_breeze.commands.sbom_commands.core_jobs", autospec=True) as mock_core_jobs,
    ):
        update_sbom_information.callback(
            airflow_root_path=airflow_root_path,
            airflow_site_archive_path=None,
            airflow_version="3.2.0",
            airflow_constraints_reference="constraints-main",
            all_combinations=False,
            debug_resources=False,
            force=False,
            github_token=None,
            include_npm=True,
            include_provider_dependencies=False,
            include_python=True,
            include_success_outputs=False,
            package_filter="apache-airflow",
            parallelism=1,
            python_versions="3.10",
            remote_name="origin",
            run_in_parallel=False,
            skip_cleanup=False,
            add_stable=True,
        )

    mock_get_active_airflow_versions.assert_not_called()
    mock_core_jobs.assert_called_once()
    assert mock_core_jobs.call_args.kwargs["airflow_constraints_reference"] == "constraints-main"


def test_update_sbom_information_with_site_archive_path_keeps_stable_lookup(tmp_path):
    airflow_site_archive_path = Path(tmp_path)

    with (
        patch("airflow_breeze.utils.cdxgen.start_cdxgen_servers", autospec=True),
        patch(
            "airflow_breeze.utils.github.get_active_airflow_versions",
            autospec=True,
            return_value=(["3.1.0"], {}),
        ) as mock_get_active_airflow_versions,
        patch("airflow_breeze.commands.sbom_commands.core_jobs", autospec=True),
    ):
        update_sbom_information.callback(
            airflow_root_path=None,
            airflow_site_archive_path=airflow_site_archive_path,
            airflow_version="3.2.0",
            airflow_constraints_reference=None,
            all_combinations=False,
            debug_resources=False,
            force=False,
            github_token=None,
            include_npm=True,
            include_provider_dependencies=False,
            include_python=True,
            include_success_outputs=False,
            package_filter="apache-airflow",
            parallelism=1,
            python_versions="3.10",
            remote_name="origin",
            run_in_parallel=False,
            skip_cleanup=False,
            add_stable=True,
        )

    mock_get_active_airflow_versions.assert_called_once_with(confirm=False, remote_name="origin")
