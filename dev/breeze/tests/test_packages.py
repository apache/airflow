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

from collections.abc import Iterable
from pathlib import Path

import pytest

from airflow_breeze.global_constants import REGULAR_DOC_PACKAGES
from airflow_breeze.utils.packages import (
    PipRequirements,
    apply_version_suffix_to_non_provider_pyproject_tomls,
    apply_version_suffix_to_provider_pyproject_toml,
    convert_cross_package_dependencies_to_table,
    convert_pip_requirements_to_table,
    expand_all_provider_distributions,
    find_matching_long_package_names,
    get_available_distributions,
    get_cross_provider_dependent_packages,
    get_dist_package_name_prefix,
    get_long_package_name,
    get_min_airflow_version,
    get_pip_package_name,
    get_provider_details,
    get_provider_info_dict,
    get_provider_requirements,
    get_removed_provider_ids,
    get_short_package_name,
    get_suspended_provider_folders,
    get_suspended_provider_ids,
    validate_provider_info_with_runtime_schema,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH


def test_get_available_packages():
    assert len(get_available_distributions()) > 70
    assert all(package not in REGULAR_DOC_PACKAGES for package in get_available_distributions())


def test_expand_all_provider_distributions():
    assert len(expand_all_provider_distributions(("all-providers",))) > 70


def test_expand_all_provider_distributions_deduplicate_with_other_packages():
    assert len(expand_all_provider_distributions(("all-providers",))) == len(
        expand_all_provider_distributions(("all-providers", "amazon", "google"))
    )


def test_get_available_packages_include_non_provider_doc_packages():
    all_packages_including_regular_docs = get_available_distributions(include_non_provider_doc_packages=True)
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" not in all_packages_including_regular_docs


def test_get_available_packages_include_non_provider_doc_packages_and_all_providers():
    all_packages_including_regular_docs = get_available_distributions(
        include_non_provider_doc_packages=True, include_all_providers=True
    )
    for package in REGULAR_DOC_PACKAGES:
        assert package in all_packages_including_regular_docs

    assert "all-providers" in all_packages_including_regular_docs


def test_get_short_package_name():
    assert get_short_package_name("apache-airflow") == "apache-airflow"
    assert get_short_package_name("docker-stack") == "docker-stack"
    assert get_short_package_name("task-sdk") == "task-sdk"
    assert get_short_package_name("apache-airflow-providers-amazon") == "amazon"
    assert get_short_package_name("apache-airflow-providers-apache-hdfs") == "apache.hdfs"


def test_error_on_get_short_package_name():
    with pytest.raises(ValueError, match="Invalid provider name"):
        get_short_package_name("wrong-provider-name")


def test_get_long_package_name():
    assert get_long_package_name("apache-airflow") == "apache-airflow"
    assert get_long_package_name("docker-stack") == "docker-stack"
    assert get_long_package_name("task-sdk") == "task-sdk"
    assert get_long_package_name("amazon") == "apache-airflow-providers-amazon"
    assert get_long_package_name("apache.hdfs") == "apache-airflow-providers-apache-hdfs"


def test_get_provider_requirements():
    # update me when asana dependencies change
    assert get_provider_requirements("asana") == [
        "apache-airflow>=2.11.0",
        "apache-airflow-providers-common-compat>=1.8.0",
        "asana>=5.0.0",
    ]


def test_get_removed_providers():
    # Modify it every time we schedule provider for removal or remove it
    assert get_removed_provider_ids() == []


def test_get_suspended_provider_ids():
    # Modify it every time we suspend/resume provider
    assert get_suspended_provider_ids() == []


def test_get_suspended_provider_folders():
    # Modify it every time we suspend/resume provider
    assert get_suspended_provider_folders() == []


@pytest.mark.parametrize(
    ("short_packages", "filters", "long_packages"),
    [
        (("amazon",), (), ("apache-airflow-providers-amazon",)),
        (("apache.hdfs",), (), ("apache-airflow-providers-apache-hdfs",)),
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


@pytest.mark.parametrize(
    ("provider_id", "pip_package_name"),
    [
        ("asana", "apache-airflow-providers-asana"),
        ("apache.hdfs", "apache-airflow-providers-apache-hdfs"),
    ],
)
def test_get_pip_package_name(provider_id: str, pip_package_name: str):
    assert get_pip_package_name(provider_id) == pip_package_name


@pytest.mark.parametrize(
    ("provider_id", "expected_package_name"),
    [
        ("asana", "apache_airflow_providers_asana"),
        ("apache.hdfs", "apache_airflow_providers_apache_hdfs"),
    ],
)
def test_get_dist_package_name_prefix(provider_id: str, expected_package_name: str):
    assert get_dist_package_name_prefix(provider_id) == expected_package_name


@pytest.mark.parametrize(
    ("requirement_string", "expected"),
    [
        pytest.param("apache-airflow", ("apache-airflow", ""), id="no-version-specifier"),
        pytest.param(
            "apache-airflow <2.7,>=2.5", ("apache-airflow", ">=2.5,<2.7"), id="range-version-specifier"
        ),
        pytest.param("watchtower~=3.0.1", ("watchtower", "~=3.0.1"), id="compat-version-specifier"),
        pytest.param("PyGithub!=1.58", ("PyGithub", "!=1.58"), id="not-equal-version-specifier"),
        pytest.param(
            "apache-airflow[amazon,google,microsoft.azure,docker]>2.7.0",
            ("apache-airflow[amazon,docker,google,microsoft.azure]", ">2.7.0"),
            id="package-with-extra",
        ),
        pytest.param(
            'mysql-connector-python>=8.0.11; platform_machine != "aarch64"',
            ("mysql-connector-python", '>=8.0.11; platform_machine != "aarch64"'),
            id="version-with-platform-marker",
        ),
        pytest.param(
            "pendulum>=2.1.2,<4.0;python_version<'3.12'",
            ("pendulum", '>=2.1.2,<4.0; python_version < "3.12"'),
            id="version-with-python-marker",
        ),
        pytest.param(
            "celery>=5.3.0,<6,!=5.3.3,!=5.3.2",
            ("celery", ">=5.3.0,!=5.3.2,!=5.3.3,<6"),
            id="complex-version-specifier",
        ),
        pytest.param(
            "apache-airflow; python_version<'3.12' or platform_machine != 'i386'",
            ("apache-airflow", '; python_version < "3.12" or platform_machine != "i386"'),
            id="no-version-specifier-with-complex-marker",
        ),
    ],
)
def test_parse_pip_requirements_parse(requirement_string: str, expected: tuple[str, str]):
    assert PipRequirements.from_requirement(requirement_string) == expected


@pytest.mark.parametrize(
    ("requirements", "markdown", "table"),
    [
        (
            ["apache-airflow>2.5.0", "apache-airflow-providers-http"],
            False,
            """
=================================  ==================
PIP package                        Version required
=================================  ==================
``apache-airflow``                 ``>2.5.0``
``apache-airflow-providers-http``
=================================  ==================
""",
        ),
        (
            ["apache-airflow>2.5.0", "apache-airflow-providers-http"],
            True,
            """
| PIP package                     | Version required   |
|:--------------------------------|:-------------------|
| `apache-airflow`                | `>2.5.0`           |
| `apache-airflow-providers-http` |                    |
""",
        ),
    ],
)
def test_convert_pip_requirements_to_table(requirements: Iterable[str], markdown: bool, table: str):
    assert convert_pip_requirements_to_table(requirements, markdown).strip() == table.strip()


def test_validate_provider_info_with_schema():
    for provider in get_available_distributions():
        validate_provider_info_with_runtime_schema(get_provider_info_dict(provider))


@pytest.mark.parametrize(
    ("provider_id", "min_version"),
    [
        ("amazon", "2.11.0"),
        ("fab", "3.0.2"),
    ],
)
def test_get_min_airflow_version(provider_id: str, min_version: str):
    assert get_min_airflow_version(provider_id) == min_version


def test_convert_cross_package_dependencies_to_table():
    EXPECTED = """
| Dependent package                                                                       | Extra           |
|:----------------------------------------------------------------------------------------|:----------------|
| [apache-airflow-providers-common-compat](https://airflow.apache.org/docs/common-compat) | `common.compat` |
| [apache-airflow-providers-common-sql](https://airflow.apache.org/docs/common-sql)       | `common.sql`    |
| [apache-airflow-providers-google](https://airflow.apache.org/docs/google)               | `google`        |
| [apache-airflow-providers-openlineage](https://airflow.apache.org/docs/openlineage)     | `openlineage`   |
"""
    assert (
        convert_cross_package_dependencies_to_table(get_cross_provider_dependent_packages("trino")).strip()
        == EXPECTED.strip()
    )


def test_get_provider_info_dict():
    provider_info_dict = get_provider_info_dict("amazon")
    assert provider_info_dict["name"] == "Amazon"
    assert provider_info_dict["package-name"] == "apache-airflow-providers-amazon"
    assert "Amazon" in provider_info_dict["description"]
    assert provider_info_dict["filesystems"] == ["airflow.providers.amazon.aws.fs.s3"]
    assert len(provider_info_dict["integrations"]) > 35
    assert len(provider_info_dict["hooks"]) > 30
    assert len(provider_info_dict["triggers"]) > 15
    assert len(provider_info_dict["operators"]) > 20
    assert len(provider_info_dict["sensors"]) > 15
    assert len(provider_info_dict["transfers"]) > 15
    assert len(provider_info_dict["extra-links"]) > 5
    assert len(provider_info_dict["connection-types"]) > 3
    assert len(provider_info_dict["notifications"]) > 2
    assert len(provider_info_dict["secrets-backends"]) > 1
    assert len(provider_info_dict["logging"]) > 1
    assert len(provider_info_dict["config"].keys()) > 1
    assert len(provider_info_dict["executors"]) > 0
    assert len(provider_info_dict["dataset-uris"]) > 0
    assert len(provider_info_dict["dataset-uris"]) > 0
    assert len(provider_info_dict["asset-uris"]) > 0


def _check_dependency_modified_properly(
    dependency: str,
    modified_dependency: str,
    version_suffix: str,
    floored_version_suffix: str,
):
    should_airflow_dependencies_be_modified = floored_version_suffix != ".post1"
    if dependency.startswith("apache-airflow"):
        if should_airflow_dependencies_be_modified:
            if ">=" in dependency:
                dependency = dependency.split(";")[0]
                modified_dependency = modified_dependency.split(";")[0]
                assert modified_dependency == f"{dependency}{floored_version_suffix}"
            elif "==" in dependency:
                dependency = dependency.split(";")[0]
                modified_dependency = modified_dependency.split(";")[0]
                assert modified_dependency == f"{dependency}{version_suffix}"
            else:
                assert modified_dependency == dependency
    else:
        assert modified_dependency == dependency


def _check_dependencies_modified_properly(
    original_toml: dict, modified_toml: dict, version_suffix: str, floored_version_suffix: str
):
    original_dependencies = original_toml["project"]["dependencies"]
    modified_dependencies = modified_toml["project"]["dependencies"]
    for i, dependency in enumerate(original_dependencies):
        modified_dependency = modified_dependencies[i]
        _check_dependency_modified_properly(
            dependency, modified_dependency, version_suffix, floored_version_suffix
        )
    if "optional-dependencies" not in original_toml["project"]:
        return
    original_optional_dependencies = original_toml["project"]["optional-dependencies"]
    modified_optional_dependencies = modified_toml["project"]["optional-dependencies"]
    for key in original_optional_dependencies.keys():
        for i, dependency in enumerate(original_optional_dependencies[key]):
            modified_dependency = modified_optional_dependencies[key][i]
            _check_dependency_modified_properly(
                dependency, modified_dependency, version_suffix, floored_version_suffix
            )


@pytest.mark.parametrize(
    ("provider_id", "version_suffix", "floored_version_suffix"),
    [
        ("google", ".dev0", ".dev0"),
        ("google", ".dev1", ".dev0"),
        ("google", ".dev1+testversion", ".dev0"),
        ("google", "rc1", "rc1"),
        ("google", "rc3+localversion34", "rc1"),
        ("google", "rc2", "rc1"),
        ("google", "a1", "a1"),
        ("google", "b3", "b1"),
        ("google", ".post1", ".post1"),
        ("google", ".post2", ".post1"),
        ("amazon", ".dev0", ".dev0"),
        ("amazon", ".dev10", ".dev0"),
        ("amazon", "rc10", "rc1"),
        ("amazon", "rc23", "rc1"),
        ("amazon", "a4", "a1"),
        ("amazon", "b8", "b1"),
        ("amazon", ".post1", ".post1"),
        ("standard", ".dev1", ".dev0"),
        ("standard", "rc1", "rc1"),
        ("standard", "rc2", "rc1"),
        ("standard", "a1", "a1"),
        ("standard", "b3", "b1"),
        ("standard", ".post1", ".post1"),
    ],
)
def test_apply_version_suffix_to_provider_pyproject_toml(
    provider_id, version_suffix, floored_version_suffix, tmp_path
):
    """
    Test the apply_version_suffix function with different version suffixes for pyproject.toml of provider.
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    from unittest.mock import patch

    # Get the original provider details
    original_provider_details = get_provider_details(provider_id)
    original_pyproject_path = original_provider_details.root_provider_path / "pyproject.toml"
    original_content = original_pyproject_path.read_text()

    # Create a temporary copy of the provider directory structure
    temp_provider_path = tmp_path / "providers" / provider_id.replace(".", "/")
    temp_provider_path.mkdir(parents=True, exist_ok=True)
    temp_pyproject_path = temp_provider_path / "pyproject.toml"
    temp_pyproject_path.write_text(original_content)

    # Mock get_provider_details to return a modified version with temporary path
    def mock_get_provider_details(provider_id: str):
        # Use NamedTuple's _replace() method to create a copy with modified root_provider_path
        return original_provider_details._replace(root_provider_path=temp_provider_path)

    with patch("airflow_breeze.utils.packages.get_provider_details", side_effect=mock_get_provider_details):
        with apply_version_suffix_to_provider_pyproject_toml(
            provider_id, version_suffix
        ) as pyproject_toml_path:
            modified_content = pyproject_toml_path.read_text()

    original_toml = tomllib.loads(original_content)
    modified_toml = tomllib.loads(modified_content)
    assert original_toml["project"]["version"] != modified_toml["project"]["version"]
    assert modified_toml["project"]["version"].endswith(version_suffix)
    _check_dependencies_modified_properly(
        original_toml, modified_toml, version_suffix, floored_version_suffix
    )


AIRFLOW_CORE_INIT_PY = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "__init__.py"
TASK_SDK_INIT_PY = AIRFLOW_ROOT_PATH / "task-sdk" / "src" / "airflow" / "sdk" / "__init__.py"
AIRFLOWCTL_INIT_PY = AIRFLOW_ROOT_PATH / "airflow-ctl" / "src" / "airflowctl" / "__init__.py"


@pytest.fixture
def lock_version_files():
    from filelock import FileLock

    lock_file = AIRFLOW_ROOT_PATH / ".version_files.lock"
    with FileLock(lock_file):
        yield
    if lock_file.exists():
        lock_file.unlink()


@pytest.mark.usefixtures("lock_version_files")
@pytest.mark.parametrize(
    ("distributions", "init_file_path", "version_suffix", "floored_version_suffix"),
    [
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, ".dev0", ".dev0"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, ".dev1+testversion34", ".dev0"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "rc2", "rc1"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "rc2.dev0+localversion35", "rc1.dev0"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "rc1", "rc1"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "a2", "a1"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "b1", "b1"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, "b1+testversion34", "b1"),
        (("airflow-core", "."), AIRFLOW_CORE_INIT_PY, ".post1", ".post1"),
        (("task-sdk",), TASK_SDK_INIT_PY, ".dev0", ".dev0"),
        (("task-sdk",), TASK_SDK_INIT_PY, "rc2", "rc1"),
        (("task-sdk",), TASK_SDK_INIT_PY, "rc13", "rc1"),
        (("task-sdk",), TASK_SDK_INIT_PY, "a1", "a1"),
        (("task-sdk",), TASK_SDK_INIT_PY, "b3", "b1"),
        (("task-sdk",), TASK_SDK_INIT_PY, ".post1", ".post1"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, ".dev0", ".dev0"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, "rc2", "rc1"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, "rc13", "rc1"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, "a1", "a1"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, "b3", "b1"),
        (("airflow-ctl",), AIRFLOWCTL_INIT_PY, ".post1", ".post1"),
    ],
)
def test_apply_version_suffix_to_non_provider_pyproject_tomls(
    distributions: tuple[str, ...],
    init_file_path: Path,
    version_suffix: str,
    floored_version_suffix: str,
    tmp_path: Path,
):
    """
    Test the apply_version_suffix function with different version suffixes for pyproject.toml of non-provider.
    """
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    distribution_paths = [AIRFLOW_ROOT_PATH / distribution for distribution in distributions]
    original_pyproject_toml_paths = [path / "pyproject.toml" for path in distribution_paths]
    original_contents = [path.read_text() for path in original_pyproject_toml_paths]
    modified_pyproject_toml_paths = [
        tmp_path / path.parent.name / path.name for path in original_pyproject_toml_paths
    ]
    for i, modified_pyproject_toml_path in enumerate(modified_pyproject_toml_paths):
        modified_pyproject_toml_path.parent.mkdir(parents=True, exist_ok=True)
        modified_pyproject_toml_path.write_text(original_pyproject_toml_paths[i].read_text())
    original_init_py = init_file_path.read_text()
    modified_init_file_path = tmp_path / init_file_path.name
    modified_init_file_path.write_text(original_init_py)

    with apply_version_suffix_to_non_provider_pyproject_tomls(
        version_suffix, modified_init_file_path, modified_pyproject_toml_paths
    ) as modified_pyproject_toml_paths:
        modified_contents = [path.read_text() for path in modified_pyproject_toml_paths]

        original_tomls = [tomllib.loads(content) for content in original_contents]
        modified_tomls = [tomllib.loads(content) for content in modified_contents]
        modified_init_py = modified_init_file_path.read_text()

    assert original_init_py != modified_init_py
    assert version_suffix in modified_init_py

    for i, original_toml in enumerate(original_tomls):
        modified_toml = modified_tomls[i]
        _check_dependencies_modified_properly(
            original_toml, modified_toml, version_suffix, floored_version_suffix
        )
