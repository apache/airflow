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

import textwrap

import pytest
from check_dependency_lower_bounds import check_pyproject_file, check_requirement, extract_requirements

WORKSPACE_NAMES = frozenset({"apache-airflow-core", "apache-airflow-devel-common"})


class TestCheckRequirement:
    @pytest.mark.parametrize(
        "dependency",
        [
            pytest.param("pyspark>=4.0.0", id="greater-or-equal"),
            pytest.param("pyspark>4.0.0", id="greater"),
            pytest.param("hatchling==1.31.0", id="pinned"),
            pytest.param("hatchling===1.31.0", id="arbitrary-equality"),
            pytest.param("hatchling~=1.31", id="compatible-release"),
            pytest.param("urllib3>=2.1.0,!=2.6.0", id="lower-bound-with-exclusion"),
            pytest.param("pydantic-ai-slim[mcp]>=2.0.0", id="extras"),
            pytest.param('fastavro>=1.10.0; python_version < "3.14"', id="marker"),
        ],
    )
    def test_no_error_when_lower_bound_present(self, dependency):
        assert check_requirement("project.dependencies", dependency, WORKSPACE_NAMES) is None

    @pytest.mark.parametrize(
        "dependency",
        [
            pytest.param("pyspark", id="bare"),
            pytest.param("pydantic-ai-slim[mcp]", id="extras"),
            pytest.param("pyspark<5.0.0", id="upper-bound-only"),
            pytest.param("pyspark!=4.1.0", id="exclusion-only"),
            pytest.param('krb5; python_version < "3.14"', id="marker-only"),
        ],
    )
    def test_error_when_lower_bound_missing(self, dependency):
        error = check_requirement("project.dependencies", dependency, WORKSPACE_NAMES)
        assert error is not None
        assert "has no lower bound" in error
        assert "[project.dependencies]" in error

    @pytest.mark.parametrize(
        "dependency",
        [
            pytest.param("apache-airflow-core", id="plain"),
            pytest.param("apache_airflow_core", id="non-canonical-name"),
            pytest.param("apache-airflow-devel-common[mypy]", id="extras"),
        ],
    )
    def test_no_error_for_workspace_distribution(self, dependency):
        assert check_requirement("dependency-groups.dev", dependency, WORKSPACE_NAMES) is None

    def test_no_error_for_direct_url_requirement(self):
        dependency = (
            "sphinx-airflow-theme@https://airflow.apache.org/sphinx-airflow-theme/"
            "sphinx_airflow_theme-0.3.13-py3-none-any.whl"
        )
        assert check_requirement("project.optional-dependencies.docs", dependency, WORKSPACE_NAMES) is None

    def test_error_for_invalid_requirement(self):
        error = check_requirement("project.dependencies", "not a requirement!", WORKSPACE_NAMES)
        assert error is not None
        assert "is not a valid requirement" in error


class TestExtractRequirements:
    def test_extracts_every_guarded_table(self):
        data = {
            "build-system": {"requires": ["hatchling"]},
            "project": {
                "dependencies": ["pyspark"],
                "optional-dependencies": {"kerberos": ["krb5"]},
            },
            "dependency-groups": {"dev": ["pytest", {"include-group": "docs"}]},
        }
        assert extract_requirements(data) == [
            ("project.dependencies", "pyspark"),
            ('project.optional-dependencies."kerberos"', "krb5"),
            ('dependency-groups."dev"', "pytest"),
            ("build-system.requires", "hatchling"),
        ]

    def test_no_requirements_when_tables_absent(self):
        assert extract_requirements({"tool": {"uv": {"required-version": ">=0.9.0"}}}) == []


class TestCheckPyprojectFile:
    def _write(self, tmp_path, content):
        path = tmp_path / "pyproject.toml"
        path.write_text(textwrap.dedent(content))
        return path

    def test_reports_every_unbounded_dependency(self, tmp_path):
        path = self._write(
            tmp_path,
            """
            [project]
            name = "apache-airflow-providers-samba"
            dependencies = ["smbprotocol>=1.5.0"]

            [project.optional-dependencies]
            "kerberos" = ["krb5", "apache-airflow-core"]

            [dependency-groups]
            dev = ["pytest"]
            """,
        )
        errors = check_pyproject_file(path, WORKSPACE_NAMES)
        assert len(errors) == 2
        assert "krb5" in errors[0]
        assert "pytest" in errors[1]

    def test_no_errors_when_all_bounded(self, tmp_path):
        path = self._write(
            tmp_path,
            """
            [project]
            name = "apache-airflow-providers-samba"
            dependencies = ["smbprotocol>=1.5.0"]

            [dependency-groups]
            dev = ["pytest>=9.1.1"]
            """,
        )
        assert check_pyproject_file(path, WORKSPACE_NAMES) == []
