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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml>=6.0.3",
#   "termcolor==2.5.0",
#   "wcmatch==8.2",
# ]
# ///
from __future__ import annotations

import sys
from pathlib import Path

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH
from termcolor import colored

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )

CONFIG_KEY = "labelPRBasedOnFilePath"

cyborg_config_path = AIRFLOW_ROOT_PATH / ".github" / "boring-cyborg.yml"
cyborg_config = yaml.safe_load(cyborg_config_path.read_text())
if CONFIG_KEY not in cyborg_config:
    raise SystemExit(f"Missing section {CONFIG_KEY}")

errors = []
# Check if all patterns in the cyborg config are existing in the repository
for label, patterns in cyborg_config[CONFIG_KEY].items():
    for pattern in patterns:
        try:
            next(Path(AIRFLOW_ROOT_PATH).glob(pattern))
            continue
        except StopIteration:
            yaml_path = f"{CONFIG_KEY}.{label}"
            errors.append(
                f"Unused pattern [{colored(pattern, 'cyan')}] in [{colored(yaml_path, 'cyan')}] section."
            )

# Check for missing providers
EXCEPTIONS = ["edge3"]
providers_root = AIRFLOW_ROOT_PATH / "providers"
for p in providers_root.glob("**/provider.yaml"):
    provider_name = str(p.parent.relative_to(providers_root)).replace("/", "-")
    expected_key = f"provider:{provider_name}"
    if provider_name not in EXCEPTIONS and expected_key not in cyborg_config[CONFIG_KEY]:
        errors.append(
            f"Provider [{colored(provider_name, 'cyan')}] is missing in [{colored(expected_key, 'cyan')}] section."
        )

# Check for missing translations
EXCEPTIONS = ["en"]
for p in AIRFLOW_ROOT_PATH.glob("airflow-core/src/airflow/ui/public/i18n/locales/*"):
    if p.is_dir():
        lang_id = p.name
        expected_key = f"translation:{lang_id}"
        if lang_id not in EXCEPTIONS and expected_key not in cyborg_config[CONFIG_KEY]:
            errors.append(
                f"Translation [{colored(lang_id, 'cyan')}] is missing in [{colored(expected_key, 'cyan')}] section."
            )

if errors:
    print(f"Found {colored(str(len(errors)), 'red')} problems:")
    print("\n".join(errors))
    print(f"Please correct the above in {cyborg_config_path}")
    sys.exit(1)
else:
    print("No found problems. Have a good day!")
