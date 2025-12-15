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

from unittest.mock import patch

from airflow_breeze.commands.release_command import find_latest_release_candidate


class TestFindLatestReleaseCandidate:
    """Test the find_latest_release_candidate function."""

    def test_find_latest_rc_single_candidate(self, tmp_path):
        """Test finding release candidate when only one exists."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create a single RC directory
        (svn_dev_repo / "3.0.5rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc1"

    def test_find_latest_rc_multiple_candidates(self, tmp_path):
        """Test finding latest release candidate when multiple exist."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create multiple RC directories
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5rc2").mkdir()
        (svn_dev_repo / "3.0.5rc3").mkdir()
        (svn_dev_repo / "3.0.5rc10").mkdir()  # Test that rc10 > rc3

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc10"

    def test_find_latest_rc_ignores_other_versions(self, tmp_path):
        """Test that function ignores RCs for other versions."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RCs for different versions
        (svn_dev_repo / "3.0.4rc1").mkdir()
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5rc2").mkdir()
        (svn_dev_repo / "3.0.6rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc2"

    def test_find_latest_rc_ignores_non_rc_directories(self, tmp_path):
        """Test that function ignores directories that don't match RC pattern."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RC directory and non-RC directories
        (svn_dev_repo / "3.0.5rc1").mkdir()
        (svn_dev_repo / "3.0.5").mkdir()  # Final release directory
        (svn_dev_repo / "some-other-dir").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result == "3.0.5rc1"

    def test_find_latest_rc_no_match(self, tmp_path):
        """Test that function returns None when no matching RC found."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        # Create RCs for different version
        (svn_dev_repo / "3.0.4rc1").mkdir()

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_directory_not_exists(self, tmp_path):
        """Test that function returns None when directory doesn't exist."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        # Don't create the directory

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_empty_directory(self, tmp_path):
        """Test that function returns None when directory is empty."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
        assert result is None

    def test_find_latest_rc_task_sdk_component(self, tmp_path):
        """Test finding release candidate for task-sdk component."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        task_sdk_dir = svn_dev_repo / "task-sdk"
        task_sdk_dir.mkdir(parents=True)

        # Create multiple Task SDK RC directories
        (task_sdk_dir / "1.0.5rc1").mkdir()
        (task_sdk_dir / "1.0.5rc2").mkdir()
        (task_sdk_dir / "1.0.5rc3").mkdir()

        result = find_latest_release_candidate("1.0.5", str(svn_dev_repo), component="task-sdk")
        assert result == "1.0.5rc3"

    def test_find_latest_rc_task_sdk_ignores_airflow_rcs(self, tmp_path):
        """Test that task-sdk component ignores airflow RCs."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)
        task_sdk_dir = svn_dev_repo / "task-sdk"
        task_sdk_dir.mkdir()

        # Create airflow RC (should be ignored)
        (svn_dev_repo / "3.0.5rc1").mkdir()
        # Create task-sdk RC
        (task_sdk_dir / "1.0.5rc1").mkdir()

        result = find_latest_release_candidate("1.0.5", str(svn_dev_repo), component="task-sdk")
        assert result == "1.0.5rc1"

    def test_find_latest_rc_handles_oserror(self, tmp_path):
        """Test that function handles OSError gracefully."""
        svn_dev_repo = tmp_path / "dev" / "airflow"
        svn_dev_repo.mkdir(parents=True)

        with patch("os.listdir", side_effect=OSError("Permission denied")):
            result = find_latest_release_candidate("3.0.5", str(svn_dev_repo), component="airflow")
            assert result is None
