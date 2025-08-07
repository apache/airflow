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
from collections.abc import Iterator

import requests
import os

K8S_DEFINITIONS = (
    "https://raw.githubusercontent.com/yannh/kubernetes-json-schema"
    "/master/v1.29.0-standalone-strict/_definitions.json"
)

K8S_GITHUB_API_DEFINITIONS_URL = "https://api.github.com/repos/yannh/kubernetes-json-schema/contents/v1.29.0-standalone-strict/_definitions.json?ref=master"

VALUES_SCHEMA_FILE = "chart/values.schema.json"

def fetch_raw_url(link: str, fallback_link: str) -> str:
    """
    Fetch the image url from GitHub
    """
    token = os.environ.get("GITHUB_TOKEN")
    headers = {}
    if token:
        headers["Authorization"] = f"token {token}"
    else:
        print("Warning: GITHUB_TOKEN not found, making unauthenticated request")
    
    response = requests.get(link, headers=headers)
    if response.status_code == 200:
        content = response.json()
        if "download_url" in content:
            return content["download_url"]
        else:
            return fallback_link
    else:
        print(f"Failed to fetch URL: {link} {response.status_code} - {response.text}")
        return fallback_link


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
k8s_defs_url = fetch_raw_url(K8S_GITHUB_API_DEFINITIONS_URL, K8S_DEFINITIONS)
defs = get_remote_schema(k8s_defs_url)

# first find refs in our schema
refs = set(find_refs(schema["properties"]))

# now we look for refs in refs
for _ in range(15):
    starting_refs = refs
    for ref in refs:
        ref_id = ref.split("/")[-1]
        remote_def = defs["definitions"].get(ref_id)
        if remote_def:
            schema["definitions"][ref_id] = remote_def
    refs = set(find_refs(schema["definitions"]))
    if refs == starting_refs:
        break
else:
    raise SystemExit("Wasn't able to find all nested references in 15 cycles")

# and finally, sort them all!
schema["definitions"] = dict(sorted(schema["definitions"].items()))

# Then write out our schema
with open(VALUES_SCHEMA_FILE, "w") as f:
    json.dump(schema, f, indent=4)
    f.write("\n")  # with a newline!
