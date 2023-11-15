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

from airflow_breeze.global_constants import REGULAR_DOC_PACKAGES
from airflow_breeze.utils.packages import (
    expand_all_provider_packages,
    find_matching_long_package_names,
    get_available_packages,
    get_documentation_package_path,
    get_install_requirements,
    get_long_package_name,
    get_package_extras,
    get_provider_details,
    get_provider_requirements,
    get_removed_provider_ids,
    get_short_package_name,
    get_source_package_path,
    get_suspended_provider_folders,
    get_suspended_provider_ids,
)
from airflow_breeze.utils.path_utils import AIRFLOW_PROVIDERS_ROOT, AIRFLOW_SOURCES_ROOT, DOCS_ROOT


def test_get_available_packages():
    assert len(get_available_packages()) > 70
    assert all(package not in REGULAR_DOC_PACKAGES for package in get_available_packages())


def test_expand_all_provider_packages():
    assert len(expand_all_provider_packages(("all-providers",))) > 70


def test_expand_all_provider_packages_deduplicate_with_other_packages():
    assert len(expand_all_provider_packages(("all-providers",))) == len(
        expand_all_provider_packages(("all-providers", "amazon", "google"))
    )


def test_get_available_packages_include_non_provider_doc_packages():
    all_packages_including_regular_docs = get_available_packages(include_non_provider_doc_packages=True)
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" not in all_packages_including_regular_docs


def test_get_available_packages_include_non_provider_doc_packages_and_all_providers():
    all_packages_including_regular_docs = get_available_packages(
        include_non_provider_doc_packages=True, include_all_providers=True
    )
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" in all_packages_including_regular_docs


def test_get_short_package_name():
    assert get_short_package_name("apache-airflow") == "apache-airflow"
    assert get_short_package_name("docker-stack") == "docker-stack"
    assert get_short_package_name("apache-airflow-providers-amazon") == "amazon"
    assert get_short_package_name("apache-airflow-providers-apache-hdfs") == "apache.hdfs"


def test_error_on_get_short_package_name():
    with pytest.raises(ValueError, match="Invalid provider name"):
        get_short_package_name("wrong-provider-name")


def test_get_long_package_name():
    assert get_long_package_name("apache-airflow") == "apache-airflow"
    assert get_long_package_name("docker-stack") == "docker-stack"
    assert get_long_package_name("amazon") == "apache-airflow-providers-amazon"
    assert get_long_package_name("apache.hdfs") == "apache-airflow-providers-apache-hdfs"


def test_get_provider_requirements():
    # update me when asana dependencies change
    assert get_provider_requirements("asana") == ["apache-airflow>=2.5.0", "asana>=0.10,<4.0.0"]


def test_get_removed_providers():
    # Modify it every time we schedule provider for removal or remove it
    assert ["qubole"] == get_removed_provider_ids()


def test_get_suspended_provider_ids():
    # Modify it every time we suspend/resume provider
    assert ["qubole"] == get_suspended_provider_ids()


def test_get_suspended_provider_folders():
    # Modify it every time we suspend/resume provider
    assert ["qubole"] == get_suspended_provider_folders()


@pytest.mark.parametrize(
    "short_packages, filters, long_packages",
    [
        (("amazon",), (), ("apache-airflow-providers-amazon",)),
        (("apache.hdfs",), (), ("apache-airflow-providers-apache-hdfs",)),
        (("amazon",), (), ("apache-airflow-providers-amazon",)),
        (
            ("apache.hdfs",),
            ("apache-airflow-providers-amazon",),
            ("apache-airflow-providers-amazon", "apache-airflow-providers-apache-hdfs"),
        ),
        (
            ("apache.hdfs",),
            ("apache-airflow-providers-ama*",),
            ("apache-airflow-providers-amazon", "apache-airflow-providers-apache-hdfs"),
        ),
    ],
)
def test_find_matching_long_package_name(
    short_packages: tuple[str, ...], filters: tuple[str, ...], long_packages: tuple[str, ...]
):
    assert find_matching_long_package_names(short_packages=short_packages, filters=filters) == long_packages


def test_find_matching_long_package_name_bad_filter():
    with pytest.raises(SystemExit, match=r"Some filters did not find any package: \['bad-filter-\*"):
        find_matching_long_package_names(short_packages=(), filters=("bad-filter-*",))


def test_get_source_package_path():
    assert get_source_package_path("apache.hdfs") == AIRFLOW_PROVIDERS_ROOT / "apache" / "hdfs"


def test_get_documentation_package_path():
    assert get_documentation_package_path("apache.hdfs") == DOCS_ROOT / "apache-airflow-providers-apache-hdfs"


def test_get_install_requirements():
    assert (
        get_install_requirements("asana", "").strip()
        == """
    apache-airflow>=2.5.0
    asana>=0.10,<4.0.0
""".strip()
    )


def test_get_package_extras():
    assert get_package_extras("google") == {
        "amazon": ["apache-airflow-providers-amazon>=2.6.0"],
        "apache.beam": ["apache-airflow-providers-apache-beam", "apache-beam[gcp]"],
        "apache.cassandra": ["apache-airflow-providers-apache-cassandra"],
        "cncf.kubernetes": ["apache-airflow-providers-cncf-kubernetes>=7.2.0"],
        "common.sql": ["apache-airflow-providers-common-sql"],
        "facebook": ["apache-airflow-providers-facebook>=2.2.0"],
        "leveldb": ["plyvel"],
        "microsoft.azure": ["apache-airflow-providers-microsoft-azure"],
        "microsoft.mssql": ["apache-airflow-providers-microsoft-mssql"],
        "mysql": ["apache-airflow-providers-mysql"],
        "openlineage": ["apache-airflow-providers-openlineage"],
        "oracle": ["apache-airflow-providers-oracle>=3.1.0"],
        "postgres": ["apache-airflow-providers-postgres"],
        "presto": ["apache-airflow-providers-presto"],
        "salesforce": ["apache-airflow-providers-salesforce"],
        "sftp": ["apache-airflow-providers-sftp"],
        "ssh": ["apache-airflow-providers-ssh"],
        "trino": ["apache-airflow-providers-trino"],
    }


def test_get_provider_details():
    provider_details = get_provider_details("asana")
    assert provider_details.provider_id == "asana"
    assert provider_details.full_package_name == "airflow.providers.asana"
    assert provider_details.pypi_package_name == "apache-airflow-providers-asana"
    assert (
        provider_details.source_provider_package_path
        == AIRFLOW_SOURCES_ROOT / "airflow" / "providers" / "asana"
    )
    assert (
        provider_details.documentation_provider_package_path == DOCS_ROOT / "apache-airflow-providers-asana"
    )
    assert "Asana" in provider_details.provider_description
    assert len(provider_details.versions) > 11
    assert provider_details.excluded_python_versions == []
    assert provider_details.plugins == []
    assert provider_details.changelog_path == provider_details.source_provider_package_path / "CHANGELOG.rst"
    assert not provider_details.removed
