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
from check_excluded_provider_markers import _check_dependency


class TestCheckDependency:
    EXCLUDED = {
        "apache-airflow-providers-amazon": ["3.14"],
        "apache-airflow-providers-google": ["3.14"],
    }

    def test_no_error_when_marker_present(self):
        dep = 'apache-airflow-providers-amazon>=9.0.0; python_version !="3.14"'
        assert _check_dependency(dep, self.EXCLUDED) == []

    def test_error_when_marker_missing(self):
        dep = "apache-airflow-providers-amazon>=9.0.0"
        errors = _check_dependency(dep, self.EXCLUDED)
        assert len(errors) == 1
        assert '!="3.14"' in errors[0]

    def test_error_when_no_marker_with_extras(self):
        dep = "apache-airflow-providers-amazon[aiobotocore]>=9.6.0"
        errors = _check_dependency(dep, self.EXCLUDED)
        assert len(errors) == 1

    def test_no_error_with_extras_and_marker(self):
        dep = 'apache-airflow-providers-amazon[aiobotocore]>=9.6.0; python_version !="3.14"'
        assert _check_dependency(dep, self.EXCLUDED) == []

    def test_no_error_for_non_excluded_provider(self):
        dep = "apache-airflow-providers-celery>=1.0.0"
        assert _check_dependency(dep, self.EXCLUDED) == []

    def test_no_error_for_non_provider_dependency(self):
        dep = "requests>=2.28"
        assert _check_dependency(dep, self.EXCLUDED) == []

    def test_invalid_requirement_ignored(self):
        assert _check_dependency("not a valid requirement!!!", self.EXCLUDED) == []

    @pytest.mark.parametrize(
        "excluded_versions",
        [
            pytest.param(["3.14"], id="single-version"),
            pytest.param(["3.14", "3.15"], id="multiple-versions"),
        ],
    )
    def test_multiple_excluded_versions(self, excluded_versions):
        excluded = {"apache-airflow-providers-amazon": excluded_versions}
        dep = "apache-airflow-providers-amazon>=9.0.0"
        errors = _check_dependency(dep, excluded)
        assert len(errors) == len(excluded_versions)

    def test_partial_marker_flags_missing_version(self):
        """If provider is excluded for 3.14 and 3.15, but only 3.14 marker exists."""
        excluded = {"apache-airflow-providers-amazon": ["3.14", "3.15"]}
        dep = 'apache-airflow-providers-amazon>=9.0.0; python_version !="3.14"'
        errors = _check_dependency(dep, excluded)
        assert len(errors) == 1
        assert "3.15" in errors[0]

    def test_and_combined_markers(self):
        dep = 'apache-airflow-providers-amazon>=9.0.0; python_version !="3.14" and python_version !="3.15"'
        excluded = {"apache-airflow-providers-amazon": ["3.14", "3.15"]}
        assert _check_dependency(dep, excluded) == []
