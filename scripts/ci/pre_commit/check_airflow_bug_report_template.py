#!/usr/bin/env python
#
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

import sys
from pathlib import Path

import yaml

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is imported
from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH, check_list_sorted, console

BUG_REPORT_TEMPLATE = (
    AIRFLOW_SOURCES_ROOT_PATH
    / ".github"
    / "ISSUE_TEMPLATE"
    / "airflow_providers_bug_report.yml"
)

DEPENDENCIES_JSON_FILE_PATH = (
    AIRFLOW_SOURCES_ROOT_PATH / "generated" / "provider_dependencies.json"
)


if __name__ == "__main__":
    errors: list[str] = []
    template = yaml.safe_load(BUG_REPORT_TEMPLATE.read_text())
    for field in template["body"]:
        attributes = field.get("attributes")
        if attributes:
            if attributes.get("label") == "Apache Airflow Provider(s)":
                check_list_sorted(
                    attributes["options"], "Apache Airflow Provider(s)", errors
                )
                all_providers = set(
                    provider.replace(".", "-")
                    for provider in yaml.safe_load(
                        DEPENDENCIES_JSON_FILE_PATH.read_text()
                    ).keys()
                )
                for provider in set(attributes["options"]):
                    if provider not in all_providers:
                        errors.append(
                            f"Provider {provider} not found in provider list "
                            f"and still present in the template.!"
                        )
                    else:
                        all_providers.remove(provider)
                if all_providers:
                    errors.append(f"Not all providers are listed: {all_providers}")
    if errors:
        console.print("[red]Errors found in the bug report template[/]")
        for error in errors:
            console.print(f"[red]{error}")
        sys.exit(1)
