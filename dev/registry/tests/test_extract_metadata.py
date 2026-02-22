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
    build_global_inheritance_map,
    count_modules_by_type,
    determine_airflow_versions,
    extract_classes_from_python_file,
    extract_integrations_as_categories,
    extract_modules_from_yaml,
    fetch_provider_inventory,
    fetch_pypi_dates,
    fetch_pypi_downloads,
    find_related_providers,
    get_module_type_base_classes,
    module_path_to_file_path,
    parse_pyproject_toml,
    read_inventory,
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
# build_global_inheritance_map
# ---------------------------------------------------------------------------
class TestBuildGlobalInheritanceMap:
    def test_scans_src_directories(self, tmp_path):
        """Builds class→bases map from provider src/ directories."""
        provider_dir = tmp_path / "amazon"
        src = provider_dir / "src" / "airflow" / "providers" / "amazon"
        src.mkdir(parents=True)
        (src / "base.py").write_text("class AwsBaseOperator(BaseOperator):\n    pass\n")
        (src / "s3.py").write_text("class S3ListOperator(AwsBaseOperator):\n    pass\n")

        result = build_global_inheritance_map([provider_dir])
        assert result["AwsBaseOperator"] == {"BaseOperator"}
        assert result["S3ListOperator"] == {"AwsBaseOperator"}

    def test_handles_subscript_bases(self, tmp_path):
        """Generic subscripts like Foo[Bar] are resolved to Foo."""
        provider_dir = tmp_path / "amazon"
        src = provider_dir / "src"
        src.mkdir(parents=True)
        (src / "ops.py").write_text("class MyOp(AwsBaseOperator[S3Hook]):\n    pass\n")

        result = build_global_inheritance_map([provider_dir])
        assert result["MyOp"] == {"AwsBaseOperator"}

    def test_skips_missing_src_dir(self, tmp_path):
        """Provider directories without src/ are silently skipped."""
        provider_dir = tmp_path / "empty_provider"
        provider_dir.mkdir()

        result = build_global_inheritance_map([provider_dir])
        assert result == {}

    def test_skips_syntax_errors(self, tmp_path):
        """Files with syntax errors are silently skipped."""
        provider_dir = tmp_path / "broken"
        src = provider_dir / "src"
        src.mkdir(parents=True)
        (src / "bad.py").write_text("def broken(:\n")
        (src / "good.py").write_text("class GoodOperator(BaseOperator):\n    pass\n")

        result = build_global_inheritance_map([provider_dir])
        assert "GoodOperator" in result
        assert len(result) == 1

    def test_first_definition_wins(self, tmp_path):
        """When the same class name appears in multiple files, the first one scanned wins."""
        provider_a = tmp_path / "alpha"
        src_a = provider_a / "src"
        src_a.mkdir(parents=True)
        (src_a / "a.py").write_text("class SharedName(BaseOperator):\n    pass\n")

        provider_b = tmp_path / "beta"
        src_b = provider_b / "src"
        src_b.mkdir(parents=True)
        (src_b / "b.py").write_text("class SharedName(BaseHook):\n    pass\n")

        result = build_global_inheritance_map([provider_a, provider_b])
        # First provider's definition wins
        assert result["SharedName"] == {"BaseOperator"}

    def test_multiple_bases(self, tmp_path):
        """Classes with multiple base classes have all bases recorded."""
        provider_dir = tmp_path / "multi"
        src = provider_dir / "src"
        src.mkdir(parents=True)
        (src / "hook.py").write_text("class MultiHook(BaseHook, LoggingMixin):\n    pass\n")

        result = build_global_inheritance_map([provider_dir])
        assert result["MultiHook"] == {"BaseHook", "LoggingMixin"}


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


# ---------------------------------------------------------------------------
# read_inventory
# ---------------------------------------------------------------------------
class TestReadInventory:
    @staticmethod
    def _make_inventory(tmp_path: Path, entries: list[str]) -> Path:
        """Build a minimal objects.inv file with the given body lines."""
        import zlib

        inv_path = tmp_path / "objects.inv"
        header = (
            b"# Sphinx inventory version 2\n"
            b"# Project: test\n"
            b"# Version: 1.0\n"
            b"# The remainder of this file is compressed using zlib.\n"
        )
        body = "\n".join(entries).encode("utf-8")
        with inv_path.open("wb") as f:
            f.write(header)
            f.write(zlib.compress(body))
        return inv_path

    def test_parses_py_class_entries(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "airflow.providers.amazon.hooks.s3.S3Hook py:class 1 _api/airflow/providers/amazon/hooks/s3/index.html#$ -",
            ],
        )
        result = read_inventory(inv_path)
        assert "airflow.providers.amazon.hooks.s3.S3Hook" in result
        url = result["airflow.providers.amazon.hooks.s3.S3Hook"]
        assert (
            url
            == "_api/airflow/providers/amazon/hooks/s3/index.html#airflow.providers.amazon.hooks.s3.S3Hook"
        )

    def test_ignores_non_class_entries(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "airflow.providers.amazon.hooks.s3 py:module 1 _api/airflow/providers/amazon/hooks/s3/index.html -",
                "some_func py:function 1 api.html#$ -",
            ],
        )
        result = read_inventory(inv_path)
        assert result == {}

    def test_dollar_replacement(self, tmp_path):
        """The $ placeholder in location is replaced with the entry name."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "my.Class py:class 1 api.html#$ -",
            ],
        )
        result = read_inventory(inv_path)
        assert result["my.Class"] == "api.html#my.Class"

    def test_literal_location(self, tmp_path):
        """Locations without $ are used as-is."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "my.Class py:class 1 api.html#my.Class -",
            ],
        )
        result = read_inventory(inv_path)
        assert result["my.Class"] == "api.html#my.Class"

    def test_empty_inventory(self, tmp_path):
        inv_path = self._make_inventory(tmp_path, [])
        result = read_inventory(inv_path)
        assert result == {}

    def test_multiple_classes(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "pkg.ClassA py:class 1 a.html#$ -",
                "pkg.ClassB py:class 1 b.html#$ -",
                "pkg.func py:function 1 c.html#$ -",
            ],
        )
        result = read_inventory(inv_path)
        assert len(result) == 2
        assert "pkg.ClassA" in result
        assert "pkg.ClassB" in result


# ---------------------------------------------------------------------------
# fetch_provider_inventory (mocked network)
# ---------------------------------------------------------------------------
class TestFetchProviderInventory:
    def test_returns_cached_file_when_fresh(self, tmp_path):
        cache_dir = tmp_path / "cache"
        pkg_dir = cache_dir / "apache-airflow-providers-amazon"
        pkg_dir.mkdir(parents=True)
        inv_file = pkg_dir / "objects.inv"
        inv_file.write_text("cached content")

        result = fetch_provider_inventory("apache-airflow-providers-amazon", cache_dir=cache_dir)
        assert result == inv_file

    @patch("extract_metadata.urllib.request.urlopen")
    def test_downloads_and_caches(self, mock_urlopen, tmp_path):
        cache_dir = tmp_path / "cache"
        content = b"# Sphinx inventory version 2\nsome data"
        mock_response = MagicMock()
        mock_response.read.return_value = content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_provider_inventory("apache-airflow-providers-amazon", cache_dir=cache_dir)
        assert result is not None
        assert result.exists()
        assert result.read_bytes() == content

    @patch("extract_metadata.urllib.request.urlopen")
    def test_returns_none_on_invalid_header(self, mock_urlopen, tmp_path):
        cache_dir = tmp_path / "cache"
        content = b"<html>Not Found</html>"
        mock_response = MagicMock()
        mock_response.read.return_value = content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_provider_inventory("apache-airflow-providers-amazon", cache_dir=cache_dir)
        assert result is None

    @patch("extract_metadata.urllib.request.urlopen", side_effect=OSError("connection refused"))
    def test_returns_none_on_network_error(self, _mock, tmp_path):
        cache_dir = tmp_path / "cache"
        result = fetch_provider_inventory("apache-airflow-providers-amazon", cache_dir=cache_dir)
        assert result is None

    @patch("extract_metadata.urllib.request.urlopen")
    def test_stale_cache_triggers_refetch(self, mock_urlopen, tmp_path):
        """When the cached file is older than the TTL, a new fetch is triggered."""
        import os
        import time

        cache_dir = tmp_path / "cache"
        pkg_dir = cache_dir / "apache-airflow-providers-amazon"
        pkg_dir.mkdir(parents=True)
        inv_file = pkg_dir / "objects.inv"
        inv_file.write_text("old content")
        # Set mtime to 13 hours ago (past the 12-hour TTL)
        old_time = time.time() - 13 * 3600
        os.utime(inv_file, (old_time, old_time))

        new_content = b"# Sphinx inventory version 2\nnew data"
        mock_response = MagicMock()
        mock_response.read.return_value = new_content
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)
        mock_urlopen.return_value = mock_response

        result = fetch_provider_inventory("apache-airflow-providers-amazon", cache_dir=cache_dir)
        assert result is not None
        assert result.read_bytes() == new_content


# ---------------------------------------------------------------------------
# extract_modules_from_yaml — integration tests
# ---------------------------------------------------------------------------
class TestExtractModulesFromYamlIntegration:
    """Integration tests for the full extraction pipeline.

    These verify that extract_modules_from_yaml correctly discovers classes
    through transitive inheritance, applies filtering, and uses inventory
    for docs URLs — exercising the interaction between AST parsing, global
    inheritance, and inventory lookup together.
    """

    @staticmethod
    def _setup_provider(tmp_path: Path) -> tuple[Path, dict[str, set[str]]]:
        """Create a fake provider directory with cross-file inheritance.

        Layout:
            tmp_path/providers/fake/
              src/airflow/providers/fake/
                base.py       — FakeBaseOperator(BaseOperator)
                operators/
                  s3.py       — FakeS3Operator(FakeBaseOperator)
                  _internal.py — _PrivateOp(BaseOperator)  (should be skipped)
                hooks/
                  conn.py     — FakeHook(BaseHook)
        """
        provider_dir = tmp_path / "providers" / "fake"
        pkg = provider_dir / "src" / "airflow" / "providers" / "fake"

        # base module with intermediate class
        pkg.mkdir(parents=True)
        (pkg / "__init__.py").write_text("")
        (pkg / "base.py").write_text(
            textwrap.dedent("""\
                class FakeBaseOperator(BaseOperator):
                    \"\"\"Base for all fake operators.\"\"\"
                    pass
            """)
        )

        # operators
        ops_dir = pkg / "operators"
        ops_dir.mkdir()
        (ops_dir / "__init__.py").write_text("")
        (ops_dir / "s3.py").write_text(
            textwrap.dedent("""\
                class FakeS3Operator(FakeBaseOperator):
                    \"\"\"Operates on fake S3.\"\"\"
                    pass

                class AbstractFakeOp(FakeBaseOperator):
                    \"\"\"Should be filtered out.\"\"\"
                    pass
            """)
        )
        (ops_dir / "_internal.py").write_text(
            textwrap.dedent("""\
                class _PrivateOp(BaseOperator):
                    pass
            """)
        )

        # hooks
        hooks_dir = pkg / "hooks"
        hooks_dir.mkdir()
        (hooks_dir / "__init__.py").write_text("")
        (hooks_dir / "conn.py").write_text(
            textwrap.dedent("""\
                class FakeHook(BaseHook):
                    \"\"\"Connects to fake service.\"\"\"
                    pass
            """)
        )

        # Build global inheritance map (same as main() does)
        global_inheritance = build_global_inheritance_map([provider_dir])
        return provider_dir, global_inheritance

    def test_transitive_inheritance_captured_in_extraction(self, tmp_path):
        """FakeS3Operator inherits FakeBaseOperator (different file) which inherits BaseOperator.

        This proves the full pipeline resolves cross-file transitive chains.
        """
        provider_dir, global_inheritance = self._setup_provider(tmp_path)

        provider_yaml = {
            "operators": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.operators.s3"],
                }
            ],
        }

        with patch("extract_metadata.PROVIDERS_DIR", tmp_path / "providers"):
            modules = extract_modules_from_yaml(
                provider_yaml,
                provider_id="fake",
                provider_name="Fake Provider",
                provider_path=provider_dir,
                global_inheritance=global_inheritance,
            )

        names = {m.name for m in modules}
        # FakeS3Operator should be found via transitive inheritance
        assert "FakeS3Operator" in names
        # FakeBaseOperator starts with "Base" prefix and should be skipped by post-filter
        # AbstractFakeOp contains "Abstract" and should be skipped
        assert "FakeBaseOperator" not in names
        assert "AbstractFakeOp" not in names

    def test_private_classes_excluded(self, tmp_path):
        """Classes starting with _ are excluded from extraction."""
        provider_dir, global_inheritance = self._setup_provider(tmp_path)

        provider_yaml = {
            "operators": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.operators._internal"],
                }
            ],
        }

        with patch("extract_metadata.PROVIDERS_DIR", tmp_path / "providers"):
            modules = extract_modules_from_yaml(
                provider_yaml,
                provider_id="fake",
                provider_name="Fake Provider",
                provider_path=provider_dir,
                global_inheritance=global_inheritance,
            )

        assert modules == []

    def test_hooks_and_operators_extracted_separately(self, tmp_path):
        """Operators and hooks are typed correctly based on provider.yaml sections."""
        provider_dir, global_inheritance = self._setup_provider(tmp_path)

        provider_yaml = {
            "operators": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.operators.s3"],
                }
            ],
            "hooks": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.hooks.conn"],
                }
            ],
        }

        with patch("extract_metadata.PROVIDERS_DIR", tmp_path / "providers"):
            modules = extract_modules_from_yaml(
                provider_yaml,
                provider_id="fake",
                provider_name="Fake Provider",
                provider_path=provider_dir,
                global_inheritance=global_inheritance,
            )

        types_by_name = {m.name: m.type for m in modules}
        assert types_by_name["FakeS3Operator"] == "operator"
        assert types_by_name["FakeHook"] == "hook"

    def test_inventory_overrides_manual_docs_url(self, tmp_path):
        """When inventory contains the class, docs_url uses the inventory path."""
        provider_dir, global_inheritance = self._setup_provider(tmp_path)

        provider_yaml = {
            "hooks": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.hooks.conn"],
                }
            ],
        }

        inventory = {
            "airflow.providers.fake.hooks.conn.FakeHook": "_api/hooks/conn.html#airflow.providers.fake.hooks.conn.FakeHook",
        }

        with patch("extract_metadata.PROVIDERS_DIR", tmp_path / "providers"):
            modules = extract_modules_from_yaml(
                provider_yaml,
                provider_id="fake",
                provider_name="Fake Provider",
                provider_path=provider_dir,
                global_inheritance=global_inheritance,
                inventory=inventory,
            )

        hook_module = next(m for m in modules if m.name == "FakeHook")
        assert "_api/hooks/conn.html#airflow.providers.fake.hooks.conn.FakeHook" in hook_module.docs_url
        # Should NOT contain the manually constructed path pattern
        assert "/_api/airflow/providers/fake/hooks/conn/index.html" not in hook_module.docs_url

    def test_fallback_url_when_not_in_inventory(self, tmp_path):
        """When inventory doesn't contain the class, falls back to manual URL construction."""
        provider_dir, global_inheritance = self._setup_provider(tmp_path)

        provider_yaml = {
            "hooks": [
                {
                    "integration-name": "Fake",
                    "python-modules": ["airflow.providers.fake.hooks.conn"],
                }
            ],
        }

        # Inventory exists but doesn't have FakeHook
        inventory = {"some.other.Class": "other.html#some.other.Class"}

        with patch("extract_metadata.PROVIDERS_DIR", tmp_path / "providers"):
            modules = extract_modules_from_yaml(
                provider_yaml,
                provider_id="fake",
                provider_name="Fake Provider",
                provider_path=provider_dir,
                global_inheritance=global_inheritance,
                inventory=inventory,
            )

        hook_module = next(m for m in modules if m.name == "FakeHook")
        # Should use the manual fallback pattern
        assert "/_api/airflow/providers/fake/hooks/conn/index.html" in hook_module.docs_url
