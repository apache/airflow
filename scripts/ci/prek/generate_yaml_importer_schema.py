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
Regenerate the YAML DAG importer schema snapshot.

The snapshot is the head-version JSON Schema generated from Pydantic models for
the head ``$schema`` version (from the Cadwyn bundle). If the committed snapshot
at ``task-sdk/src/airflow/sdk/importers/yaml_importer/schema.json`` differs from
the freshly generated content, the hook rewrites it and exits non-zero.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"

from airflow.sdk.importers.yaml_importer.models import DagDocument
from airflow.sdk.importers.yaml_importer.versions import get_bundle

SNAPSHOT_PATH = (
    Path(__file__)
    .parents[3]
    .joinpath("task-sdk", "src", "airflow", "sdk", "importers", "yaml_importer", "schema.json")
)


def build_schema() -> str:
    """Assemble the published head-version JSON Schema from the pydantic models."""
    head = get_bundle().versions[0].value  # newest-first
    schema = DagDocument.model_json_schema(by_alias=True)
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["title"] = f"Airflow YAML DAG ({head})"
    return json.dumps(schema, indent=2) + "\n"


def main() -> int:
    new_content = build_schema()
    if SNAPSHOT_PATH.exists() and SNAPSHOT_PATH.read_text() == new_content:
        return 0
    SNAPSHOT_PATH.write_text(new_content)
    print(f"Regenerated {SNAPSHOT_PATH.name}. Please review the diff and re-stage the file.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
