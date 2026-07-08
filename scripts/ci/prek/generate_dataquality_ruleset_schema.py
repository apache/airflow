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
"""
Regenerate the Data Quality RuleSet JSON schema snapshot at
``providers/dataquality/src/airflow/providers/dataquality/skills/dataquality-rule-authoring/references/ruleset.schema.json``.
"""

from __future__ import annotations

import subprocess
import sys

from common_prek_utils import AIRFLOW_ROOT_PATH, console

DQ_PROVIDER_PATH = AIRFLOW_ROOT_PATH / "providers" / "dataquality"
SCHEMA_PATH = DQ_PROVIDER_PATH.joinpath(
    "src",
    "airflow",
    "providers",
    "dataquality",
    "skills",
    "dataquality-rule-authoring",
    "references",
    "ruleset.schema.json",
)

DUMP_SCHEMA = r"""
import json
import sys

from airflow.providers.dataquality.rules import RuleSet

sys.stdout.write(json.dumps(RuleSet.model_json_schema(), indent=2))
sys.stdout.write("\n")
"""


def dump_schema() -> str:
    """Run the schema-dump snippet in the dataquality provider project and return its stdout."""
    result = subprocess.run(
        [
            "uv",
            "run",
            "--frozen",
            "--no-progress",
            "--project",
            str(DQ_PROVIDER_PATH),
            "python",
            "-c",
            DUMP_SCHEMA,
        ],
        cwd=DQ_PROVIDER_PATH,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Schema generation failed: {result.stderr}")
    return result.stdout


def main() -> int:
    try:
        new_content = dump_schema()
    except Exception as e:
        console.print(f"[bold red]ERROR:[/] {e}")
        return 1

    if SCHEMA_PATH.exists():
        old_content = SCHEMA_PATH.read_text()
        if old_content == new_content:
            return 0
    else:
        SCHEMA_PATH.parent.mkdir(parents=True, exist_ok=True)

    SCHEMA_PATH.write_text(new_content)
    rel = SCHEMA_PATH.relative_to(AIRFLOW_ROOT_PATH)
    console.print(f"[yellow]Regenerated[/] [cyan]{rel}[/]. Please review the diff and re-stage the file.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
