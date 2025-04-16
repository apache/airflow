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


def get_latest_helm_chart_version():
    import requests

    response = requests.get("https://airflow.apache.org/_gen/packages-metadata.json")
    data = response.json()
    for package in data:
        if package["package-name"] == "helm-chart":
            stable_version = package["stable-version"]
            return stable_version


def get_latest_airflow_version():
    import requests

    response = requests.get("https://pypi.org/pypi/apache-airflow/json")
    response.raise_for_status()
    latest_released_version = response.json()["info"]["version"]
    return latest_released_version


def get_package_version_suffix(version_suffix_for_pypi: str, version_suffix_for_local: str) -> str:
    """
    Creates a package version by combining the version suffix for PyPI and the version suffix for local. If
    either one is an empty string, it is ignored. If the local suffix does not have a leading plus sign,
    the leading plus sign will be added.

    Args:
        version_suffix_for_pypi (str): The version suffix for PyPI.
        version_suffix_for_local (str): The version suffix for local.

    Returns:
        str: The combined package version.

    """
    # if there is no local version suffix, return the PyPi version suffix
    if not version_suffix_for_local:
        return version_suffix_for_pypi

    # ensure the local version suffix starts with a plus sign
    if version_suffix_for_local[0] != "+":
        version_suffix_for_local = "+" + version_suffix_for_local

    # if there is a PyPi version suffix, return the combined version. Otherwise just return the local version.
    if version_suffix_for_pypi:
        return version_suffix_for_pypi + version_suffix_for_local
    return version_suffix_for_local


def remove_local_version_suffix(version_suffix: str) -> str:
    if "+" in version_suffix:
        return version_suffix.split("+")[0]
    return version_suffix


def is_local_package_version(version_suffix: str) -> bool:
    """
    Check if the given version suffix is a local version suffix. A local version suffix will contain a
    plus sign ('+'). This function does not guarantee that the version suffix is a valid local version suffix.

    Args:
        version_suffix (str): The version suffix to check.

    Returns:
        bool: True if the version suffix contains a '+', False otherwise. Please note this does not
        guarantee that the version suffix is a valid local version suffix.
    """
    if version_suffix and ("+" in version_suffix):
        return True
    return False
