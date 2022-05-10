#!/usr/bin/env python3

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

'''
Take normal chart CHANGELOG entries and build ArtifactHub changelog annotations.
Only outputs the annotations for the latest release in the CHANGELOG.

e.g from:

New Features
""""""""""""

- Add resources for `cleanup` and `createuser` jobs (#19263)

to:

- kind: added
  description: Add resources for `cleanup` and `createuser` jobs
  links:
    - name: "#19263"
      url: https://github.com/apache/airflow/pull/19263
'''


import re
from typing import Dict, List, Optional, Tuple, Union

import yaml

TYPE_MAPPING = {
    # CHANGELOG: (ArtifactHub kind, prefix for description)
    # ArtifactHub kind must be one of: added, changed, deprecated, removed, fixed or security
    "New Features": ("added", None),
    "Improvements": ("changed", None),
    "Bug Fixes": ("fixed", None),
    "Doc only changes": ("changed", "Docs"),
    "Misc": ("changed", "Misc"),
}

PREFIXES_TO_STRIP = [
    # case insensitive
    "Chart:",
    "Chart Docs:",
]


def parse_line(line: str) -> Tuple[Optional[str], Optional[int]]:
    match = re.search(r'^- (.*?)(?:\(#(\d+)\)){0,1}$', line)
    if not match:
        return None, None
    desc, pr_number = match.groups()
    return desc.strip(), int(pr_number)


def print_entry(section: str, description: str, pr_number: Optional[int]):
    for unwanted_prefix in PREFIXES_TO_STRIP:
        if description.lower().startswith(unwanted_prefix.lower()):
            description = description[len(unwanted_prefix) :].strip()

    kind, prefix = TYPE_MAPPING[section]
    if prefix:
        description = f"{prefix}: {description}"
    entry: Dict[str, Union[str, List]] = {"kind": kind, "description": description}
    if pr_number:
        entry["links"] = [
            {"name": f"#{pr_number}", "url": f"https://github.com/apache/airflow/pull/{pr_number}"}
        ]
    print(yaml.dump([entry]))


in_first_release = False
past_significant_changes = False
section = ""
with open("chart/RELEASE_NOTES.rst") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        if line.startswith("Airflow Helm Chart"):
            # We only want to get annotations for the "latest" release
            if in_first_release:
                break
            in_first_release = True
            continue
        if line.startswith('"""') or line.startswith('----') or line.startswith('^^^^'):
            continue

        # Make sure we get past "significant features" before we actually start keeping track
        if not past_significant_changes:
            if line == "New Features":
                section = line
                past_significant_changes = True
            continue

        if not line.startswith('- '):
            section = line
            continue

        description, pr = parse_line(line)
        if description:
            print_entry(section, description, pr)
