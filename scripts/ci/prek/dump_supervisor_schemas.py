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
Dump the supervisor schema snapshot. Prints JSON to stdout.

Mirrors :mod:`scripts.ci.prek.generate_execution_api_schema` but for the
supervisor schema ``VersionBundle``: walks the registered head models and
emits ``model_json_schema()`` for every wire body in a deterministic
class-name order so the artefact diffs cleanly across runs.

Run with cwd at the repo root.
"""

from __future__ import annotations

import json
import os
import sys

from pydantic import json_schema

os.environ["_AIRFLOW__AS_LIBRARY"] = "1"

from airflow.sdk.execution_time.schema import bundle, registered_models_by_name

# Models are sorted by name before being passed to generation for stable diffs.
_, snapshot = json_schema.models_json_schema(
    [(m, "serialization") for _, m in sorted(registered_models_by_name().items())],
    schema_generator=json_schema.GenerateJsonSchema,
)
snapshot = {
    "$schema": json_schema.GenerateJsonSchema.schema_dialect,
    "api_version": str(bundle.versions[0].value),
    "description": "Apache Airflow SDK Supervisor Schema",
    "$defs": snapshot["$defs"],
}

# We currently still have references to non-SDK models from SDK. This cause
# issues since Pydantic would see two classes for one (logical) model. This
# post-process Pydantic's output to merge those class pairs into one. Eventually
# we should have clean separation between SDK and Core, and won't need this
# anymore. But that is still quite some way away.
renames: dict[str, str] = {}
for key, val in (defs := snapshot["$defs"]).items():
    if (title := (val.get("title") or key)) != key:
        renames[key] = title

for key in list(defs):
    try:
        title = renames[key]
    except KeyError:
        continue
    entry = defs.pop(key)
    if title in defs:  # Dup, don't need this one.
        continue
    defs[title] = entry

output = json.dumps(snapshot, indent=2)
for old, new in renames.items():
    output = output.replace(f'"#/$defs/{old}"', f'"#/$defs/{new}"')


sys.stdout.write(output)
sys.stdout.write("\n")
