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

import argparse

MIN_AIRFLOW_VERSION_BY_PYTHON = {
    "3.10": "2.11.0",
    "3.13": "3.1.0",
    "3.14": "3.2.0",
}


def _version_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def get_min_airflow_version_for_python(python_version: str) -> str:
    """
    Return the minimum supported Airflow version for the given Python version.

    Unknown future Python versions inherit the latest known minimum Airflow version so the
    requirement never decreases when a new Python version is added to the DB test matrix.
    """

    matching_versions = [
        version
        for version in MIN_AIRFLOW_VERSION_BY_PYTHON
        if _version_key(version) <= _version_key(python_version)
    ]
    if not matching_versions:
        raise ValueError(f"No minimum Airflow version defined for Python {python_version}")
    return MIN_AIRFLOW_VERSION_BY_PYTHON[max(matching_versions, key=_version_key)]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Return the minimum supported Airflow version for a Python version."
    )
    parser.add_argument("python_version", help="Python major.minor version, for example 3.14")
    args = parser.parse_args()
    print(get_min_airflow_version_for_python(args.python_version))


if __name__ == "__main__":
    main()
