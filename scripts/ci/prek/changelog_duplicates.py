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
"""
Make sure there aren't duplicate entries in changelogs
"""

from __future__ import annotations

import re
import sys

# These are known exceptions, for example where the PR was present in multiple releases
known_exceptions = [
    "14738",  # Both in 1.10.15 and 2.0.2
    "6550",  # Commits tagged to the same PR for both 1.10.7 and 1.10.12
    "6367",  # Commits tagged to the same PR for both 1.10.7 and 1.10.8
    "6062",  # Commits tagged to the same PR for both 1.10.10 and 1.10.11
    "3946",  # Commits tagged to the same PR for both 1.10.2 and 1.10.3
    "4260",  # Commits tagged to the same PR for both 1.10.2 and 1.10.3
    "13153",  # Both a bugfix and a feature
]

pr_number_re = re.compile(r".*\(#([0-9]{1,6})\)`?`?$")

files = sys.argv[1:]

failed = False
for filename in files:
    seen = []
    dups = []
    with open(filename) as f:
        for line in f:
            match = pr_number_re.search(line)
            if match:
                pr_number = match.group(1)
                if pr_number not in seen:
                    seen.append(pr_number)
                elif pr_number not in known_exceptions:
                    dups.append(pr_number)

    if dups:
        print(f"Duplicate changelog entries found for {filename}: {dups}")
        failed = True

if failed:
    sys.exit(1)
