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

import json
from typing import Iterator

import requests

K8S_DEFINITIONS = (
    "https://raw.githubusercontent.com/yannh/kubernetes-json-schema"
    "/master/v1.22.0-standalone-strict/_definitions.json"
)
VALUES_SCHEMA_FILE = "chart/values.schema.json"


with open(VALUES_SCHEMA_FILE) as f:
    schema = json.load(f)


def find_refs(props: dict) -> Iterator[str]:
    for value in props.values():
        if "$ref" in value:
            yield value["$ref"]

        if "items" in value:
            if "$ref" in value["items"]:
                yield value["items"]["$ref"]

        if "properties" in value:
            yield from find_refs(value["properties"])


def get_remote_schema(url: str) -> dict:
    req = requests.get(url)
    req.raise_for_status()
    return req.json()


# Create 'definitions' if it doesn't exist or reset the io.k8s defs
schema["definitions"] = {k: v for k, v in schema.get("definitions", {}).items() if not k.startswith("io.k8s")}

# Get the k8s defs
defs = get_remote_schema(K8S_DEFINITIONS)

# first find refs in our schema
refs = set(find_refs(schema["properties"]))

# now we look for refs in refs
i = 0
while True:
    starting_refs = refs
    for ref in refs:
        ref_id = ref.split("/")[-1]
        remote_def = defs["definitions"].get(ref_id)
        if not remote_def:
            continue
        schema["definitions"][ref_id] = remote_def
    refs = set(find_refs(schema["definitions"]))
    if refs == starting_refs:
        break

    # Make sure we don't have a runaway loop
    i += 1
    if i > 15:
        raise SystemExit("Wasn't able to find all nested references in 15 cycles")

# and finally, sort them all!
schema["definitions"] = dict(sorted(schema["definitions"].items()))

# Then write out our schema
with open(VALUES_SCHEMA_FILE, "w") as f:
    json.dump(schema, f, indent=4)
    f.write("\n")  # with a newline!
