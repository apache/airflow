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

import os
from unittest.mock import patch

import pytest

from airflow_breeze.utils.docs_version_validation import error_versions, validate_docs_version


class TestValidateDocsVersion:
    def setup_method(self):
        os.environ["AIRFLOW_SITE_DIRECTORY"] = "/path/to/docs-archive"
        error_versions.clear()

    @patch("os.listdir")
    @patch("os.path.join")
    def test_validate_docs_version_with_invalid_versions(self, mock_path_join, mock_listdir):
        mock_listdir.side_effect = [
            ["apache-airflow", "apache-airflow-providers-google"],
            ["1.10.0", "stable", "invalid_version"],
            ["2.0.0", "stable", "stable.txt"],
        ]
        mock_path_join.side_effect = lambda *args: "/".join(args)

        with pytest.raises(SystemExit):
            validate_docs_version()
        assert "Invalid version: 'invalid_version' found under doc folder apache-airflow" in error_versions

    @patch("os.listdir")
    @patch("os.path.join")
    def test_validate_docs_version_with_valid_versions(self, mock_path_join, mock_listdir):
        mock_listdir.side_effect = [
            ["apache-airflow", "apache-airflow-providers-standard"],
            ["1.10.0", "stable"],
            ["2.0.0", "stable", "stable.txt"],
        ]
        mock_path_join.side_effect = lambda *args: "/".join(args)
        validate_docs_version()
        assert not error_versions, f"No errors should be found for valid versions, {error_versions}"
