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
"""Unit tests for dev/registry/extract_metadata.py."""

from __future__ import annotations

import http.client
import json
import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from extract_metadata import (
    count_modules_by_type,
    determine_airflow_versions,
    extract_classes_from_python_file,
    extract_integrations_as_categories,
    fetch_pypi_dates,
    fetch_pypi_downloads,
    find_related_providers,
    get_module_type_base_classes,
    module_path_to_file_path,
    parse_pyproject_toml,
)


# ---------------------------------------------------------------------------
# get_module_type_base_classes
# ---------------------------------------------------------------------------
class TestGetModuleTypeBaseClasses:
    def test_returns_all_expected_keys(self):
        result = get_module_type_base_classes()
        assert set(result.keys()) == {"operator", "hook", "sensor", "trigger", "transfer", "bundle"}

    def test_every_value_is_nonempty_set(self):
        for module_type, base_classes in get_module_type_base_classes().items():
            assert isinstance(base_classes, set), f"{module_type} should be a set"
            assert len(base_classes) > 0, f"{module_type} should not be empty"


# ---------------------------------------------------------------------------
# module_path_to_file_path
# ---------------------------------------------------------------------------
class TestModulePathToFilePath:
    @pytest.mark.parametrize(
        ("module_path", "provider_path", "expected_suffix"),
        [
            (
                "airflow.providers.amazon.operators.s3",
                Path("/repo/providers/amazon"),
                Path("/repo/providers/amazon/src/airflow/providers/amazon/operators/s3.py"),
            ),
            (
                "airflow.providers.microsoft.azure.hooks.wasb",
                Path("/repo/providers/microsoft/azure"),
                Path("/repo/providers/microsoft/azure/src/airflow/providers/microsoft/azure/hooks/wasb.py"),
            ),
            (
                "airflow.providers.foo.bar",
                Path("/tmp/custom"),
                Path("/tmp/custom/src/airflow/providers/foo/bar.py"),
            ),
        ],
        ids=["amazon-operator", "nested-provider", "arbitrary-base"],
    )
    def test_conversion(self, module_path, provider_path, expected_suffix):
        assert module_path_to_file_path(module_path, provider_path) == expected_suffix


# ---------------------------------------------------------------------------
# extract_integrations_as_categories
# ---------------------------------------------------------------------------
class TestExtractIntegrationsAsCategories:
    def test_empty_yaml_returns_empty_list(self):
        assert extract_integrations_as_categories({}) == []

    def test_single_integration(self):
        yaml_data = {"integrations": [{"integration-name": "Amazon S3"}]}
        categories = extract_integrations_as_categories(yaml_data)
        assert len(categories) == 1
        assert categories[0].name == "Amazon S3"
        assert categories[0].id == "amazon-s3"

    def test_special_characters_stripped(self):
        yaml_data = {"integrations": [{"integration-name": "Google Cloud (GCS)"}]}
        categories = extract_integrations_as_categories(yaml_data)
        assert categories[0].id == "google-cloud-gcs"

    def test_duplicate_names_deduplicated(self):
        yaml_data = {
            "integrations": [
                {"integration-name": "Amazon S3"},
                {"integration-name": "Amazon S3"},
            ]
        }
        categories = extract_integrations_as_categories(yaml_data)
        assert len(categories) == 1


# ---------------------------------------------------------------------------
# count_modules_by_type
# ---------------------------------------------------------------------------
class TestCountModulesByType:
    def test_empty_yaml_returns_all_zero(self):
        counts = count_modules_by_type({})
        assert len(counts) == 11
        assert all(v == 0 for v in counts.values())

    def test_operators_only(self):
        yaml_data = {
            "operators": [
                {"python-modules": ["mod1", "mod2"]},
                {"python-modules": ["mod3"]},
            ]
        }
        counts = count_modules_by_type(yaml_data)
        assert counts["operator"] == 3
        assert counts["hook"] == 0

    def test_mixed_module_types(self):
        yaml_data = {
            "operators": [{"python-modules": ["op1"]}],
            "hooks": [{"python-modules": ["h1", "h2"]}],
            "transfers": [{"source": "a", "target": "b"}],
            "notifications": ["notifier.Class"],
            "task-decorators": [{"name": "my_task", "class-name": "mod.func"}],
        }
        counts = count_modules_by_type(yaml_data)
        assert counts["operator"] == 1
        assert counts["hook"] == 2
        assert counts["transfer"] == 1
        assert counts["notifier"] == 1
        assert counts["decorator"] == 1
        assert counts["sensor"] == 0


# ---------------------------------------------------------------------------
# determine_airflow_versions
# ---------------------------------------------------------------------------
class TestDetermineAirflowVersions:
    @pytest.mark.parametrize(
        ("deps", "expected"),
        [
            (["apache-airflow>=2.11.0", "some-other-dep"], ["2.11+"]),
            (["apache-airflow>=3.0.0,<4.0"], ["3.0+"]),
            (["unrelated-dep>=1.0"], ["3.0+"]),
            ([], ["3.0+"]),
        ],
        ids=["airflow-2.11", "airflow-3.0", "no-airflow-dep", "empty-list"],
    )
    def test_version_detection(self, deps, expected):
        assert determine_airflow_versions(deps) == expected


# ---------------------------------------------------------------------------
# find_related_providers
# ---------------------------------------------------------------------------
class TestFindRelatedProviders:
    def test_no_overlap_returns_empty(self):
        yamls = {
            "alpha": {"integrations": [{"integration-name": "Foo"}]},
            "beta": {"integrations": [{"integration-name": "Bar"}]},
        }
        assert find_related_providers("alpha", yamls) == []

    def test_shared_integration_found(self):
        yamls = {
            "alpha": {"integrations": [{"integration-name": "S3"}]},
            "beta": {"integrations": [{"integration-name": "S3"}]},
        }
        assert find_related_providers("alpha", yamls) == ["beta"]

    def test_self_excluded(self):
        yamls = {
            "alpha": {"integrations": [{"integration-name": "S3"}]},
        }
        assert find_related_providers("alpha", yamls) == []

    def test_capped_at_five(self):
        yamls = {
            "main": {"integrations": [{"integration-name": "Common"}]},
        }
        for i in range(10):
            yamls[f"other-{i}"] = {"integrations": [{"integration-name": "Common"}]}
        result = find_related_providers("main", yamls)
        assert len(result) <= 5


# ---------------------------------------------------------------------------
# extract_classes_from_python_file
# ---------------------------------------------------------------------------
class TestExtractClassesFromPythonFile:
    def test_nonexistent_file(self):
        assert extract_classes_from_python_file(Path("/no/such/file.py"), {"BaseOperator"}) == []

    def test_empty_file(self, tmp_path):
        f = tmp_path / "empty.py"
        f.write_text("")
        assert extract_classes_from_python_file(f, {"BaseOperator"}) == []

    def test_syntax_error_file(self, tmp_path):
        f = tmp_path / "bad.py"
        f.write_text("def broken(:\n")
        assert extract_classes_from_python_file(f, {"BaseOperator"}) == []

    def test_matching_class_with_docstring(self, tmp_path):
        f = tmp_path / "my_op.py"
        f.write_text(
            textwrap.dedent("""\
                class MyOperator(BaseOperator):
                    \"\"\"Does something useful.\"\"\"
                    pass
            """)
        )
        result = extract_classes_from_python_file(f, {"BaseOperator"})
        assert len(result) == 1
        assert result[0]["name"] == "MyOperator"
        assert result[0]["docstring"] == "Does something useful."
        assert result[0]["line"] == 1

    def test_no_matching_base_class(self, tmp_path):
        f = tmp_path / "unrelated.py"
        f.write_text(
            textwrap.dedent("""\
                class Unrelated(SomethingElse):
                    pass
            """)
        )
        assert extract_classes_from_python_file(f, {"BaseOperator"}) == []

    def test_attribute_base_class(self, tmp_path):
        f = tmp_path / "hook.py"
        f.write_text(
            textwrap.dedent("""\
                class MyHook(module.BaseHook):
                    \"\"\"A hook.\"\"\"
                    pass
            """)
        )
        result = extract_classes_from_python_file(f, {"BaseHook"})
        assert len(result) == 1
        assert result[0]["name"] == "MyHook"

    def test_transitive_inheritance_within_file(self, tmp_path):
        """Classes inheriting from an intermediate class defined in the same file are found."""
        f = tmp_path / "sql.py"
        f.write_text(
            textwrap.dedent("""\
                class BaseSQLOperator(BaseOperator):
                    \"\"\"Base class for SQL operators.\"\"\"
                    pass

                class SQLExecuteQueryOperator(BaseSQLOperator):
                    \"\"\"Executes SQL queries.\"\"\"
                    pass

                class SQLCheckOperator(BaseSQLOperator):
                    \"\"\"Checks SQL results.\"\"\"
                    pass
            """)
        )
        result = extract_classes_from_python_file(f, {"BaseOperator"})
        names = {r["name"] for r in result}
        assert names == {"BaseSQLOperator", "SQLExecuteQueryOperator", "SQLCheckOperator"}

    def test_transitive_inheritance_deep_chain(self, tmp_path):
        """Three levels of inheritance are resolved."""
        f = tmp_path / "deep.py"
        f.write_text(
            textwrap.dedent("""\
                class Mid(BaseOperator):
                    pass
                class Leaf(Mid):
                    pass
            """)
        )
        result = extract_classes_from_python_file(f, {"BaseOperator"})
        names = {r["name"] for r in result}
        assert names == {"Mid", "Leaf"}

    def test_no_classes_returns_empty(self, tmp_path):
        """Files with only functions and no classes return empty list."""
        f = tmp_path / "handlers.py"
        f.write_text(
            textwrap.dedent("""\
                def fetch_all_handler(cursor):
                    return cursor.fetchall()
            """)
        )
        assert extract_classes_from_python_file(f, {"BaseHook"}) == []

    def test_unrelated_class_not_included(self, tmp_path):
        """Classes inheriting from unrelated bases are excluded."""
        f = tmp_path / "lineage.py"
        f.write_text(
            textwrap.dedent("""\
                from enum import Enum
                class SqlJobHookLineageExtra(str, Enum):
                    pass
            """)
        )
        assert extract_classes_from_python_file(f, {"BaseHook"}) == []

    def test_empty_base_classes_returns_all(self, tmp_path):
        """When base_classes is empty, all classes are returned."""
        f = tmp_path / "bundle.py"
        f.write_text(
            textwrap.dedent("""\
                class MyBundle:
                    pass
                class AnotherBundle:
                    pass
            """)
        )
        result = extract_classes_from_python_file(f, set())
        assert len(result) == 2

    def test_generic_subscript_base_class(self, tmp_path):
        """Classes using generic subscript syntax like Foo[Bar] are found."""
        f = tmp_path / "s3.py"
        f.write_text(
            textwrap.dedent("""\
                class S3CreateBucketOperator(AwsBaseOperator[S3Hook]):
                    \"\"\"Create an S3 bucket.\"\"\"
                    pass
            """)
        )
        # AwsBaseOperator inherits from BaseOperator via global inheritance
        global_inh = {"AwsBaseOperator": {"BaseOperator"}}
        result = extract_classes_from_python_file(f, {"BaseOperator"}, global_inh)
        assert len(result) == 1
        assert result[0]["name"] == "S3CreateBucketOperator"

    def test_cross_file_inheritance_via_global_map(self, tmp_path):
        """Classes whose parent is only known via global_inheritance are found."""
        f = tmp_path / "ec2.py"
        f.write_text(
            textwrap.dedent("""\
                class EC2StartInstanceOperator(AwsBaseOperator):
                    \"\"\"Start an EC2 instance.\"\"\"
                    pass
            """)
        )
        global_inh = {"AwsBaseOperator": {"BaseOperator"}}
        result = extract_classes_from_python_file(f, {"BaseOperator"}, global_inh)
        assert len(result) == 1
        assert result[0]["name"] == "EC2StartInstanceOperator"


# ---------------------------------------------------------------------------
# parse_pyproject_toml
# ---------------------------------------------------------------------------
class TestParsePyprojectToml:
    def test_nonexistent_path(self, tmp_path):
        result = parse_pyproject_toml(tmp_path / "nonexistent.toml")
        assert result == {"requires_python": "", "dependencies": [], "optional_extras": {}}

    def test_basic_toml(self, tmp_path):
        toml_file = tmp_path / "pyproject.toml"
        toml_file.write_text(
            textwrap.dedent("""\
                [project]
                requires-python = ">=3.10"
                dependencies = [
                    "apache-airflow>=3.0.0",
                    "boto3>=1.28.0",
                ]
            """)
        )
        result = parse_pyproject_toml(toml_file)
        assert result["requires_python"] == ">=3.10"
        assert result["dependencies"] == ["apache-airflow>=3.0.0", "boto3>=1.28.0"]

    def test_optional_dependencies(self, tmp_path):
        toml_file = tmp_path / "pyproject.toml"
        toml_file.write_text(
            textwrap.dedent("""\
                [project]
                requires-python = ">=3.10"
                dependencies = []

                [project.optional-dependencies]
                cncf-kubernetes = ["kubernetes>=21.7.0"]
            """)
        )
        result = parse_pyproject_toml(toml_file)
        assert "cncf-kubernetes" in result["optional_extras"]
        assert result["optional_extras"]["cncf-kubernetes"] == ["kubernetes>=21.7.0"]

    def test_dependencies_capped_at_twenty(self, tmp_path):
        toml_file = tmp_path / "pyproject.toml"
        deps = ", ".join(f'"dep{i}>=1.0"' for i in range(25))
        toml_file.write_text(
            textwrap.dedent(f"""\
                [project]
                dependencies = [{deps}]
            """)
        )
        result = parse_pyproject_toml(toml_file)
        assert len(result["dependencies"]) == 20


# ---------------------------------------------------------------------------
# fetch_pypi_downloads (mocked network)
# ---------------------------------------------------------------------------
class TestFetchPypiDownloads:
    @patch("extract_metadata.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        payload = json.dumps({"data": {"last_week": 500, "last_month": 2000}}).encode()
        mock_response = MagicMock(spec=http.client.HTTPResponse)
        mock_response.read.return_value = payload
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_pypi_downloads("apache-airflow-providers-amazon")
        assert result["weekly"] == 500
        assert result["monthly"] == 2000
        assert result["total"] == 0

    @patch("extract_metadata.urllib.request.urlopen", side_effect=OSError("timeout"))
    def test_network_error(self, _mock):
        result = fetch_pypi_downloads("nonexistent-package")
        assert result == {"weekly": 0, "monthly": 0, "total": 0}


# ---------------------------------------------------------------------------
# fetch_pypi_dates (mocked network)
# ---------------------------------------------------------------------------
class TestFetchPypiDates:
    @patch("extract_metadata.urllib.request.urlopen")
    def test_success(self, mock_urlopen):
        payload = json.dumps(
            {
                "releases": {
                    "1.0.0": [{"upload_time_iso_8601": "2021-03-15T12:00:00Z"}],
                    "2.0.0": [{"upload_time_iso_8601": "2024-01-20T08:00:00Z"}],
                }
            }
        ).encode()
        mock_response = MagicMock(spec=http.client.HTTPResponse)
        mock_response.read.return_value = payload
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_pypi_dates("apache-airflow-providers-amazon")
        assert result["first_released"] == "2021-03-15"
        assert result["last_updated"] == "2024-01-20"

    @patch("extract_metadata.urllib.request.urlopen", side_effect=OSError("timeout"))
    def test_network_error(self, _mock):
        result = fetch_pypi_dates("nonexistent-package")
        assert result == {"first_released": "", "last_updated": ""}
