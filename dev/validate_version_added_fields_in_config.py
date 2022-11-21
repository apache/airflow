#!/usr/bin/env python
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

import functools
import sys
from pathlib import Path
from pprint import pprint

import requests
import semver
import yaml

ROOT_DIR = Path(__file__).resolve().parent / ".."

KNOWN_FALSE_DETECTIONS = {
    # This option has been added in v2.0.0, but we had mistake in config.yml file until v2.2.0.
    # https://github.com/apache/airflow/pull/17808
    ("logging", "extra_logger_names", "2.2.0")
}


def fetch_pypi_versions() -> list[str]:
    r = requests.get("https://pypi.org/pypi/apache-airflow/json")
    r.raise_for_status()
    all_version = r.json()["releases"].keys()
    released_versions = [d for d in all_version if not (("rc" in d) or ("b" in d))]
    return released_versions


@functools.lru_cache()
def fetch_config_options_for_version(version: str) -> set[tuple[str, str]]:
    r = requests.get(
        f"https://raw.githubusercontent.com/apache/airflow/{version}/airflow/config_templates/config.yml"
    )
    r.raise_for_status()
    config_sections = yaml.safe_load(r.text)
    config_options = {
        (
            config_section["name"],
            config_option["name"],
        )
        for config_section in config_sections
        for config_option in config_section["options"]
    }
    return config_options


def read_local_config_options() -> set[tuple[str, str, str]]:
    config_sections = yaml.safe_load((ROOT_DIR / "airflow" / "config_templates" / "config.yml").read_text())
    config_options = {
        (config_section["name"], config_option["name"], config_option["version_added"])
        for config_section in config_sections
        for config_option in config_section["options"]
    }
    return config_options


# 1. Prepare versions to checks
airflow_version = fetch_pypi_versions()
airflow_version = sorted(airflow_version, key=semver.VersionInfo.parse)
to_check_versions: list[str] = [d for d in airflow_version if d.startswith("2.")]

# 2. Compute expected options set with version added fields
expected_computed_options: set[tuple[str, str, str]] = set()
for prev_version, curr_version in zip(to_check_versions[:-1], to_check_versions[1:]):
    print("Processing version:", curr_version)
    options_1 = fetch_config_options_for_version(prev_version)
    options_2 = fetch_config_options_for_version(curr_version)
    new_options = options_2 - options_1
    expected_computed_options.update(
        {(section_name, option_name, curr_version) for section_name, option_name in new_options}
    )
print("Expected computed options count:", len(expected_computed_options))

# 3. Read local options set
local_options = read_local_config_options()
print("Local options count:", len(local_options))

# 4. Hide options that do not exist in the local configuration file. They are probably deprecated.
local_options_plain: set[tuple[str, str]] = {
    (section_name, option_name) for section_name, option_name, version_added in local_options
}
computed_options: set[tuple[str, str, str]] = {
    (section_name, option_name, version_added)
    for section_name, option_name, version_added in expected_computed_options
    if (section_name, option_name) in local_options_plain
}
print("Visible computed options count:", len(computed_options))

# 5. Compute difference between expected and local options set
local_options_with_version_added: set[tuple[str, str, str]] = {
    (section_name, option_name, version_added)
    for section_name, option_name, version_added in local_options
    if version_added
}
diff_options: set[tuple[str, str, str]] = computed_options - local_options_with_version_added

diff_options -= KNOWN_FALSE_DETECTIONS

if diff_options:
    pprint(diff_options)
    sys.exit(1)
else:
    print("No changes required")
