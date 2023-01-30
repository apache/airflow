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

import re

import requests
import toml

# The list of users in the 'build-info' job looks like:
#
#       contains(fromJSON('[
#         "BasPH",
#         ...
#       ]'), github.event.pull_request.user.login)
#
# This script should replace the contents of the array with a new list of
# identically formatted names, such that changes to the source of truth:
AUTHORS = "https://raw.githubusercontent.com/apache/airflow-ci-infra/main/authors.toml"

# ...end up being reflected in the ci.yml:
WORKFLOW = ".github/workflows/ci.yml"


req = requests.get(AUTHORS)
author_list = toml.loads(req.text)

author_set = set()
for membership in author_list:
    author_set.update([author for author in author_list[membership]])

authors = ""
for author in sorted(author_set):
    authors += f'            "{author}",\n'

authors = authors[:-2]

with open(WORKFLOW) as handle:
    new_ci = re.sub(
        r"""
            ^
            # matches the entire file up to contains(fromJSON('[
            ( .*? contains.fromJSON \( ' \[ \n )

            # the list of authors (which is replaced)
            .*?

            # the remainder of the file, from the end of the list onwards
            ( \s+ \] ' \), . github\.event\.pull_request\.user\.login .*? )
            $
        """,
        f"\\1{authors}\\2",
        handle.read(),
        flags=re.DOTALL | re.VERBOSE,
    )

with open(WORKFLOW, "w") as handle:
    handle.write(new_ci)
