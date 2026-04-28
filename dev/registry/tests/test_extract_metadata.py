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
    determine_airflow_versions,
    extract_integrations_as_categories,
    fetch_provider_inventory,
    fetch_pypi_dates,
    fetch_pypi_downloads,
    find_latest_released_version,
    find_related_providers,
    load_release_tags,
    module_path_to_file_path,
    parse_pyproject_toml,
    read_connection_urls,
    read_inventory,
    resolve_connection_docs_url,
)


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
# read_connection_urls
# ---------------------------------------------------------------------------
class TestReadConnectionUrls:
    @staticmethod
    def _make_inventory(tmp_path: Path, entries: list[str]) -> Path:
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

    def test_parses_std_label_entries(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "howto/connection:kubernetes std:label -1 connections/kubernetes.html#howto-connection-kubernetes Kubernetes cluster Connection",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result == {"kubernetes": "connections/kubernetes.html"}

    def test_parses_std_doc_entries(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "connections/tableau std:doc -1 connections/tableau.html Tableau Connection",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result == {"tableau": "connections/tableau.html"}

    def test_label_takes_precedence_over_doc(self, tmp_path):
        """When both std:label and std:doc exist for the same key, label wins."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "howto/connection:aws std:label -1 connections/aws.html#howto-connection-aws AWS Connection",
                "connections/aws std:doc -1 connections/aws.html AWS Connection",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result["aws"] == "connections/aws.html"

    def test_skips_sub_section_labels(self, tmp_path):
        """Labels like howto/connection:gcp:configuring_the_connection are sub-sections, not top-level."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "howto/connection:gcp std:label -1 connections/gcp.html#howto-connection-gcp GCP Connection",
                "howto/connection:gcp:configuring_the_connection std:label -1 connections/gcp.html#sub Configuring",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result == {"gcp": "connections/gcp.html"}

    def test_skips_connections_index(self, tmp_path):
        """The connections/index doc should not appear in the map."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "connections/index std:doc -1 connections/index.html Connection Types",
                "connections/kafka std:doc -1 connections/kafka.html Kafka Connection",
            ],
        )
        result = read_connection_urls(inv_path)
        assert "index" not in result
        assert result == {"kafka": "connections/kafka.html"}

    def test_ignores_unrelated_entries(self, tmp_path):
        inv_path = self._make_inventory(
            tmp_path,
            [
                "airflow.providers.amazon.hooks.s3.S3Hook py:class 1 api.html#$ -",
                "some_module py:module 1 mod.html -",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result == {}

    def test_empty_inventory(self, tmp_path):
        inv_path = self._make_inventory(tmp_path, [])
        result = read_connection_urls(inv_path)
        assert result == {}

    def test_multiple_connection_types(self, tmp_path):
        """Amazon-style provider with multiple connection pages."""
        inv_path = self._make_inventory(
            tmp_path,
            [
                "howto/connection:aws std:label -1 connections/aws.html#howto-connection-aws AWS",
                "howto/connection:emr std:label -1 connections/emr.html#howto-connection-emr EMR",
                "howto/connection:redshift std:label -1 connections/redshift.html#howto-connection-redshift Redshift",
                "connections/athena std:doc -1 connections/athena.html Athena",
            ],
        )
        result = read_connection_urls(inv_path)
        assert result["aws"] == "connections/aws.html"
        assert result["emr"] == "connections/emr.html"
        assert result["redshift"] == "connections/redshift.html"
        assert result["athena"] == "connections/athena.html"


# ---------------------------------------------------------------------------
# resolve_connection_docs_url
# ---------------------------------------------------------------------------
class TestResolveConnectionDocsUrl:
    BASE = "https://airflow.apache.org/docs/apache-airflow-providers-google/stable"

    def test_exact_match(self):
        conn_map = {"kubernetes": "connections/kubernetes.html"}
        url = resolve_connection_docs_url("kubernetes", conn_map, self.BASE)
        assert url == f"{self.BASE}/connections/kubernetes.html"

    def test_fallback_to_connections_dir(self):
        conn_map = {"kubernetes": "connections/kubernetes.html"}
        url = resolve_connection_docs_url("unknown_type", conn_map, self.BASE)
        assert url == f"{self.BASE}/connections/"

    def test_empty_map_falls_back_to_connections_dir(self):
        url = resolve_connection_docs_url("aws", {}, self.BASE)
        assert url == f"{self.BASE}/connections/"

    def test_google_bigquery_resolves(self):
        """gcpbigquery conn_type should resolve to bigquery.html, not index."""
        conn_map = {
            "gcp": "connections/gcp.html",
            "gcpbigquery": "connections/bigquery.html",
        }
        url = resolve_connection_docs_url("gcpbigquery", conn_map, self.BASE)
        assert url == f"{self.BASE}/connections/bigquery.html"

    def test_tableau_resolves(self):
        conn_map = {"tableau": "connections/tableau.html"}
        url = resolve_connection_docs_url("tableau", conn_map, self.BASE)
        assert url == f"{self.BASE}/connections/tableau.html"


# ---------------------------------------------------------------------------
# load_release_tags
# ---------------------------------------------------------------------------
class TestLoadReleaseTags:
    def test_parses_subprocess_output(self):
        from unittest.mock import MagicMock, patch

        mock_result = MagicMock()
        mock_result.stdout = (
            "providers-amazon/9.25.0\n"
            "providers-amazon/9.26.0\n"
            "providers-celery/3.18.0\n"
            "providers-celery/3.19.0rc1\n"
            "\n"  # blank line
            "  providers-google/21.2.0  \n"  # whitespace tolerated
        )
        with patch("extract_metadata.subprocess.run", return_value=mock_result) as mock_run:
            tags = load_release_tags()

        assert tags == {
            "providers-amazon/9.25.0",
            "providers-amazon/9.26.0",
            "providers-celery/3.18.0",
            "providers-celery/3.19.0rc1",
            "providers-google/21.2.0",
        }
        # The git command runs against the providers-* glob
        cmd = mock_run.call_args[0][0]
        assert cmd[:3] == ["git", "tag", "--list"]
        assert cmd[3] == "providers-*"

    def test_returns_empty_set_on_subprocess_failure(self):
        from subprocess import CalledProcessError
        from unittest.mock import patch

        with patch(
            "extract_metadata.subprocess.run",
            side_effect=CalledProcessError(1, ["git", "tag", "--list"]),
        ):
            tags = load_release_tags()
        assert tags == set()

    def test_returns_empty_set_when_git_not_installed(self):
        from unittest.mock import patch

        with patch("extract_metadata.subprocess.run", side_effect=FileNotFoundError):
            tags = load_release_tags()
        assert tags == set()


# ---------------------------------------------------------------------------
# find_latest_released_version
# ---------------------------------------------------------------------------
class TestFindLatestReleasedVersion:
    def test_returns_top_when_top_has_tag(self):
        tags = {"providers-amazon/9.26.0", "providers-amazon/9.25.0"}
        assert find_latest_released_version("amazon", ["9.26.0", "9.25.0"], tags) == "9.26.0"

    def test_walks_past_phantom_top(self):
        # celery 3.19.0 is in versions: but no final tag -- only rc1.
        tags = {
            "providers-celery/3.19.0rc1",
            "providers-celery/3.18.0",
            "providers-celery/3.17.2",
        }
        result = find_latest_released_version("celery", ["3.19.0", "3.18.0", "3.17.2"], tags)
        assert result == "3.18.0"

    def test_returns_none_when_no_versions_have_tags(self):
        # akeyless: brand-new provider, listed in versions: but never tagged.
        tags = {"providers-amazon/9.26.0"}  # different provider
        result = find_latest_released_version("akeyless", ["1.0.0"], tags)
        assert result is None

    def test_returns_none_for_empty_versions_list(self):
        result = find_latest_released_version("amazon", [], {"providers-amazon/9.26.0"})
        assert result is None

    def test_rc_only_treated_as_phantom(self):
        # Final 3.19.0 is missing; rc1/rc2 exist. Final must match exactly.
        tags = {"providers-celery/3.19.0rc1", "providers-celery/3.19.0rc2", "providers-celery/3.18.0"}
        # versions: list contains only the would-be final
        result = find_latest_released_version("celery", ["3.19.0"], tags)
        assert result is None
        # When fallback also exists in versions, returns the fallback
        result = find_latest_released_version("celery", ["3.19.0", "3.18.0"], tags)
        assert result == "3.18.0"

    def test_does_not_match_other_providers_tags(self):
        # provider id is part of the tag prefix; pure version coincidence shouldn't match
        tags = {"providers-google/9.26.0"}
        result = find_latest_released_version("amazon", ["9.26.0"], tags)
        assert result is None


# ---------------------------------------------------------------------------
# Filter behaviour applied to the `versions` list (not just the latest pointer)
# ---------------------------------------------------------------------------
class TestVersionsListFiltering:
    """Regression test for the bug where Provider.version (singular) was
    filtered to a real release but Provider.versions (list) still contained
    phantom entries. Downstream consumers like extract_versions.py read the
    list and would chase non-existent backfill tags.
    """

    def test_filter_drops_phantom_top_from_list(self):
        # This mirrors the in-loop logic. We don't have to test main()
        # end-to-end -- the filter is a single comprehension that we can
        # exercise directly to lock in the contract.
        provider_id = "celery"
        raw_versions = ["3.19.0", "3.18.0", "3.17.2"]
        release_tags = {
            "providers-celery/3.19.0rc1",  # not the final
            "providers-celery/3.18.0",
            "providers-celery/3.17.2",
        }
        filtered = [v for v in raw_versions if f"providers-{provider_id}/{v}" in release_tags]
        assert filtered == ["3.18.0", "3.17.2"]
        # And the latest pointer agrees
        assert find_latest_released_version(provider_id, raw_versions, release_tags) == "3.18.0"

    def test_filter_drops_unreleased_provider(self):
        provider_id = "akeyless"
        raw_versions = ["1.0.0"]
        release_tags = {"providers-amazon/9.26.0"}  # different provider
        filtered = [v for v in raw_versions if f"providers-{provider_id}/{v}" in release_tags]
        assert filtered == []
        assert find_latest_released_version(provider_id, raw_versions, release_tags) is None

    def test_filter_preserves_order(self):
        provider_id = "amazon"
        raw_versions = ["9.27.0", "9.26.0", "9.25.0", "9.24.0"]  # 9.27.0 phantom
        release_tags = {
            "providers-amazon/9.26.0",
            "providers-amazon/9.25.0",
            "providers-amazon/9.24.0",
        }
        filtered = [v for v in raw_versions if f"providers-{provider_id}/{v}" in release_tags]
        # Order from raw_versions is preserved; only the phantom is dropped
        assert filtered == ["9.26.0", "9.25.0", "9.24.0"]
