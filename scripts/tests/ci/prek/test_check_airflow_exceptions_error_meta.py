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
from check_airflow_exceptions_error_meta import get_package_name, validate_files


class TestCheckAirflowExcErrorMeta:
    def setup_method(self):
        self.exc_without_err_meta = """
from airflow._shared.exceptions import AirflowErrorCodeMixin

class ExcTestWithoutErrMetaException(AirflowErrorCodeMixin, Exception):
    pass

raise ExcTestWithoutErrMetaException"""

        self.exc_with_err_meta = """
from airflow._shared.exceptions import AirflowErrorCodeMixin

class ExcTestWithErrMetaException(AirflowErrorCodeMixin, Exception):
    description = "test description"
    documentation = "https://airflow.apache.org/docs/apache-airflow/stable/"
    first_steps = "test first steps"
    user_facing_error_message = "test user facing message"

raise ExcTestWithErrMetaException"""

    @pytest.mark.parametrize(
        "package_name, expected_errors",
        [
            (
                "airflow-core",
                [
                    "[airflow-core] description is missing in exception class ExcTestWithoutErrMetaException.",
                    "[airflow-core] documentation is missing in exception class ExcTestWithoutErrMetaException.",
                    "[airflow-core] first_steps is missing in exception class ExcTestWithoutErrMetaException.",
                    "[airflow-core] user_facing_error_message is missing in exception class ExcTestWithoutErrMetaException.",
                ],
            ),
            (
                "task-sdk",
                [
                    "[task-sdk] description is missing in exception class ExcTestWithoutErrMetaException.",
                    "[task-sdk] documentation is missing in exception class ExcTestWithoutErrMetaException.",
                    "[task-sdk] first_steps is missing in exception class ExcTestWithoutErrMetaException.",
                    "[task-sdk] user_facing_error_message is missing in exception class ExcTestWithoutErrMetaException.",
                ],
            ),
        ],
    )
    @mock.patch("check_airflow_exceptions_error_meta.get_package_name")
    def test_raise_with_exc_without_err_meta(
        self,
        mock_get_package_name,
        tmp_path,
        package_name,
        expected_errors,
    ):
        mock_get_package_name.return_value = package_name
        f = tmp_path / "fake_module.py"
        f.write_text(f"{self.exc_without_err_meta}('this is a test')")
        errors = validate_files(paths=[f])
        assert len(errors) == len(expected_errors)
        actual_errors = list(sorted(errors))
        for idx in range(len(actual_errors)):
            assert expected_errors[idx] in actual_errors[idx]

    @pytest.mark.parametrize("package_name, num_errors", [("airflow-core", 0), ("task-sdk", 0)])
    @mock.patch("check_airflow_exceptions_error_meta.get_package_name")
    def test_raise_with_exc_with_err_meta(self, mock_get_package_name, tmp_path, package_name, num_errors):
        mock_get_package_name.return_value = package_name
        f = tmp_path / "fake_module.py"
        f.write_text(f"{self.exc_with_err_meta}('this is a test')")
        errors = validate_files(paths=[f])
        assert len(errors) == num_errors

    @pytest.mark.parametrize(
        "path_suffix, exc_type, raise_match, expected_result",
        [
            ("some-package", ValueError, "Unsupported package name", None),
            ("core", None, "", "airflow-core"),
            ("airflow-core", None, "", "airflow-core"),
            ("sdk", None, "", "task-sdk"),
            ("task-sdk", None, "", "task-sdk"),
            ("providers", None, "", "providers"),
        ],
    )
    def test_get_package_name(self, tmp_path, path_suffix, exc_type, raise_match, expected_result):
        package_path = tmp_path / path_suffix
        if exc_type is None:
            actual_result = get_package_name(package_path)
            assert actual_result == expected_result
        else:
            with pytest.raises(exc_type, match=raise_match):
                get_package_name(package_path)
