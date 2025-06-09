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

from common_precommit_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_ROOT_PATH

OPTIONS_INDENT = " " * 8


def update_translation_issue_template(issue_template_path, json_files):
    with open(issue_template_path, "r+", encoding="utf-8") as f:
        lines = f.readlines()
        options_idx = next((i for i, line in enumerate(lines) if line.strip() == "options:"), None)
        if options_idx is not None:
            lines = lines[: options_idx + 1] + [f"{OPTIONS_INDENT}- label: {f.name}\n" for f in json_files]

        f.seek(0)
        f.writelines(lines)
        f.truncate()


if __name__ == "__main__":
    en_dir = AIRFLOW_CORE_SOURCES_PATH / "airflow/ui/src/i18n/locales/en"
    issue_path = AIRFLOW_ROOT_PATH / ".github/ISSUE_TEMPLATE/7-translation.yml"
    json_files = sorted(en_dir.glob("*.json"))
    update_translation_issue_template(issue_path, json_files)
