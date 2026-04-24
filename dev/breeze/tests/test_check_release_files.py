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

from pathlib import Path

from airflow_breeze.utils.check_release_files import (
    PROVIDERS_DOCKER,
    check_airflow_ctl_release,
    check_airflow_release,
    check_providers,
    check_python_client_release,
    check_task_sdk_release,
)


def test_check_airflow_release_pass():
    """Passes if all files are present."""
    files = [
        "apache_airflow-2.8.1-py3-none-any.whl",
        "apache_airflow-2.8.1-py3-none-any.whl.asc",
        "apache_airflow-2.8.1-py3-none-any.whl.sha512",
        "apache_airflow-2.8.1-source.tar.gz",
        "apache_airflow-2.8.1-source.tar.gz.asc",
        "apache_airflow-2.8.1-source.tar.gz.sha512",
        "apache_airflow-2.8.1.tar.gz",
        "apache_airflow-2.8.1.tar.gz.asc",
        "apache_airflow-2.8.1.tar.gz.sha512",
        "apache_airflow_core-2.8.1-py3-none-any.whl",
        "apache_airflow_core-2.8.1-py3-none-any.whl.asc",
        "apache_airflow_core-2.8.1-py3-none-any.whl.sha512",
        "apache_airflow_core-2.8.1.tar.gz",
        "apache_airflow_core-2.8.1.tar.gz.asc",
        "apache_airflow_core-2.8.1.tar.gz.sha512",
    ]
    assert check_airflow_release(files, version="2.8.1rc2") == []


def test_check_airflow_release_fail():
    """Fails if missing one file."""
    files = [
        "apache_airflow-2.8.1-py3-none-any.whl",
        "apache_airflow-2.8.1-py3-none-any.whl.asc",
        "apache_airflow-2.8.1-py3-none-any.whl.sha512",
        "apache_airflow-2.8.1-source.tar.gz",
        "apache_airflow-2.8.1-source.tar.gz.asc",
        "apache_airflow-2.8.1-source.tar.gz.sha512",
        "apache_airflow-2.8.1.tar.gz.asc",
        "apache_airflow-2.8.1.tar.gz.sha512",
        "apache_airflow_core-2.8.1-py3-none-any.whl",
        "apache_airflow_core-2.8.1-py3-none-any.whl.asc",
        "apache_airflow_core-2.8.1-py3-none-any.whl.sha512",
        "apache_airflow_core-2.8.1.tar.gz.asc",
        "apache_airflow_core-2.8.1.tar.gz.sha512",
    ]

    missing_files = check_airflow_release(files, version="2.8.1rc2")
    assert missing_files == ["apache_airflow-2.8.1.tar.gz", "apache_airflow_core-2.8.1.tar.gz"]


def test_check_providers_pass(tmp_path: Path):
    """Passes if all provider files are present."""
    packages_file = tmp_path / "packages.txt"
    packages_file.write_text(
        "https://pypi.org/project/apache-airflow-providers-airbyte/3.1.0rc1/\n"
        "https://pypi.org/project/apache-airflow-providers-foo-bar/9.6.42rc2/\n"
    )

    packages = [
        ("apache-airflow-providers-airbyte", "3.1.0rc1"),
        ("apache-airflow-providers-foo-bar", "9.6.42rc2"),
    ]

    files = [
        "apache_airflow_providers-2024-01-01-source.tar.gz",
        "apache_airflow_providers-2024-01-01-source.tar.gz.asc",
        "apache_airflow_providers-2024-01-01-source.tar.gz.sha512",
        "apache_airflow_providers_airbyte-3.1.0.tar.gz",
        "apache_airflow_providers_airbyte-3.1.0.tar.gz.asc",
        "apache_airflow_providers_airbyte-3.1.0.tar.gz.sha512",
        "apache_airflow_providers_airbyte-3.1.0-py3-none-any.whl",
        "apache_airflow_providers_airbyte-3.1.0-py3-none-any.whl.asc",
        "apache_airflow_providers_airbyte-3.1.0-py3-none-any.whl.sha512",
        "apache_airflow_providers_foo_bar-9.6.42.tar.gz",
        "apache_airflow_providers_foo_bar-9.6.42.tar.gz.asc",
        "apache_airflow_providers_foo_bar-9.6.42.tar.gz.sha512",
        "apache_airflow_providers_foo_bar-9.6.42-py3-none-any.whl",
        "apache_airflow_providers_foo_bar-9.6.42-py3-none-any.whl.asc",
        "apache_airflow_providers_foo_bar-9.6.42-py3-none-any.whl.sha512",
    ]
    assert check_providers(files, release_date="2024-01-01", packages=packages) == []


def test_check_providers_failure(tmp_path: Path):
    """Fails if provider files are missing."""
    packages_file = tmp_path / "packages.txt"
    packages_file.write_text("https://pypi.org/project/apache-airflow-providers-spam-egg/1.2.3rc4/\n")

    packages = [("apache-airflow-providers-spam-egg", "1.2.3rc4")]

    files = [
        "apache_airflow_providers-2024-02-01-source.tar.gz",
        "apache_airflow_providers-2024-02-01-source.tar.gz.asc",
        "apache_airflow_providers-2024-02-01-source.tar.gz.sha512",
        "apache_airflow_providers_spam_egg-1.2.3.tar.gz",
        "apache_airflow_providers_spam_egg-1.2.3.tar.gz.sha512",
        "apache_airflow_providers_spam_egg-1.2.3-py3-none-any.whl",
        "apache_airflow_providers_spam_egg-1.2.3-py3-none-any.whl.asc",
    ]
    assert sorted(check_providers(files, release_date="2024-02-01", packages=packages)) == [
        "apache_airflow_providers_spam_egg-1.2.3-py3-none-any.whl.sha512",
        "apache_airflow_providers_spam_egg-1.2.3.tar.gz.asc",
    ]


def test_check_task_sdk_release_pass():
    """Passes if all task-sdk files are present."""
    files = [
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl",
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl.asc",
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl.sha512",
        "apache_airflow_task_sdk-1.0.0.tar.gz",
        "apache_airflow_task_sdk-1.0.0.tar.gz.asc",
        "apache_airflow_task_sdk-1.0.0.tar.gz.sha512",
    ]
    assert check_task_sdk_release(files, version="1.0.0rc1") == []


def test_check_task_sdk_release_fail():
    """Fails if task-sdk files are missing."""
    files = [
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl",
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl.sha512",
        "apache_airflow_task_sdk-1.0.0.tar.gz",
        "apache_airflow_task_sdk-1.0.0.tar.gz.asc",
    ]
    missing_files = check_task_sdk_release(files, version="1.0.0rc1")
    assert sorted(missing_files) == [
        "apache_airflow_task_sdk-1.0.0-py3-none-any.whl.asc",
        "apache_airflow_task_sdk-1.0.0.tar.gz.sha512",
    ]


def test_check_airflow_ctl_release_pass():
    """Passes if all airflow-ctl files are present."""
    files = [
        "apache_airflow_ctl-1.2.3-py3-none-any.whl",
        "apache_airflow_ctl-1.2.3-py3-none-any.whl.asc",
        "apache_airflow_ctl-1.2.3-py3-none-any.whl.sha512",
        "apache_airflow_ctl-1.2.3-source.tar.gz",
        "apache_airflow_ctl-1.2.3-source.tar.gz.asc",
        "apache_airflow_ctl-1.2.3-source.tar.gz.sha512",
        "apache_airflow_ctl-1.2.3.tar.gz",
        "apache_airflow_ctl-1.2.3.tar.gz.asc",
        "apache_airflow_ctl-1.2.3.tar.gz.sha512",
    ]
    assert check_airflow_ctl_release(files, version="1.2.3rc2") == []


def test_check_airflow_ctl_release_fail():
    """Fails if airflow-ctl files are missing."""
    files = [
        "apache_airflow_ctl-1.2.3-py3-none-any.whl",
        "apache_airflow_ctl-1.2.3-py3-none-any.whl.asc",
        "apache_airflow_ctl-1.2.3-source.tar.gz.asc",
        "apache_airflow_ctl-1.2.3.tar.gz",
        "apache_airflow_ctl-1.2.3.tar.gz.sha512",
    ]
    missing_files = check_airflow_ctl_release(files, version="1.2.3rc2")
    assert sorted(missing_files) == [
        "apache_airflow_ctl-1.2.3-py3-none-any.whl.sha512",
        "apache_airflow_ctl-1.2.3-source.tar.gz",
        "apache_airflow_ctl-1.2.3-source.tar.gz.sha512",
        "apache_airflow_ctl-1.2.3.tar.gz.asc",
    ]


def test_check_python_client_release_pass():
    """Passes if all python-client files are present."""
    files = [
        "apache_airflow_client-2.5.0-py3-none-any.whl",
        "apache_airflow_client-2.5.0-py3-none-any.whl.asc",
        "apache_airflow_client-2.5.0-py3-none-any.whl.sha512",
        "apache_airflow_client-2.5.0.tar.gz",
        "apache_airflow_client-2.5.0.tar.gz.asc",
        "apache_airflow_client-2.5.0.tar.gz.sha512",
        "apache_airflow_python_client-2.5.0-source.tar.gz",
        "apache_airflow_python_client-2.5.0-source.tar.gz.asc",
        "apache_airflow_python_client-2.5.0-source.tar.gz.sha512",
    ]
    assert check_python_client_release(files, version="2.5.0rc3") == []


def test_check_python_client_release_fail():
    """Fails if python-client files are missing."""
    files = [
        "apache_airflow_client-2.5.0-py3-none-any.whl",
        "apache_airflow_client-2.5.0-py3-none-any.whl.sha512",
        "apache_airflow_client-2.5.0.tar.gz.asc",
        "apache_airflow_python_client-2.5.0-source.tar.gz",
        "apache_airflow_python_client-2.5.0-source.tar.gz.sha512",
    ]
    missing_files = check_python_client_release(files, version="2.5.0rc3")
    assert sorted(missing_files) == [
        "apache_airflow_client-2.5.0-py3-none-any.whl.asc",
        "apache_airflow_client-2.5.0.tar.gz",
        "apache_airflow_client-2.5.0.tar.gz.sha512",
        "apache_airflow_python_client-2.5.0-source.tar.gz.asc",
    ]


def test_providers_docker_upgrades_uv_before_install():
    """The generated Dockerfile.pmc must upgrade uv to satisfy the copied
    pyproject.toml's [tool.uv] required-version floor before `uv pip install`
    runs, otherwise the install step fails with a version-pin error when the
    CI image ships an older uv.
    """
    install_cmd = "RUN uv pip install --pre --system 'apache-airflow-providers-amazon==9.26.0rc1'"
    rendered = PROVIDERS_DOCKER.format(install_cmd)

    assert "pip install --upgrade 'uv>=" in rendered
    upgrade_idx = rendered.index("pip install --upgrade 'uv>=")
    install_idx = rendered.index(install_cmd)
    assert upgrade_idx < install_idx, "uv upgrade must precede uv pip install"


def test_providers_docker_uv_version_matches_required_version():
    """The hard-coded uv floor in PROVIDERS_DOCKER must equal the current
    [tool.uv] required-version from the root pyproject.toml. The
    `sync-uv-min-version-markers` prek hook enforces this at commit time;
    this test is a belt-and-braces guard for accidental drift.
    """
    from airflow_breeze.utils.check_release_files import _PROVIDERS_DOCKER_UV_MIN_VERSION
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    pyproject = (AIRFLOW_ROOT_PATH / "pyproject.toml").read_text()
    import re

    match = re.search(r'required-version\s*=\s*"\s*>=\s*([0-9]+(?:\.[0-9]+){0,2})\s*"', pyproject)
    assert match, "Could not find [tool.uv] required-version in root pyproject.toml"
    assert match.group(1) == _PROVIDERS_DOCKER_UV_MIN_VERSION
