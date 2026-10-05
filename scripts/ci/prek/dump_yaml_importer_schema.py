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
Dump the YAML DAG importer's published JSON Schema. Prints JSON to stdout.

Mirrors :mod:`scripts.ci.prek.dump_supervisor_schemas` but for the YAML DAG format: the
schema is generated from the format's pydantic models (`DagDocument`) for the head `$schema` version (from
the Cadwyn bundle). The models are the source of truth; the assembly lives here so the
production package carries no schema-generation code, mirroring the exec-API / supervisor dumps. Run with cwd at the repo root.
"""

from __future__ import annotations

import json
import os
import sys

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"

from airflow.sdk.importers.yaml_importer.models import DagDocument
from airflow.sdk.importers.yaml_importer.versions import get_bundle

# Assemble the published schema here (not in the production package): the head-version
# JSON Schema from the pydantic models, decorated with the dialect.
head = get_bundle().versions[0].value  # newest-first
schema = DagDocument.model_json_schema(by_alias=True)
schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
schema["title"] = f"Airflow YAML DAG ({head})"

sys.stdout.write(json.dumps(schema, indent=2))
sys.stdout.write("\n")
