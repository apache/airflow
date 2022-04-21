#!/usr/bin/env python3
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
Check things about newsfragments:
  - Only a single line, except for `significant` changes which can have a blank line, then the body
"""

import sys

files = sys.argv[1:]

failed = False
for filename in files:
    with open(filename) as f:
        lines = [line.strip() for line in f.readlines()]
    num_lines = len(lines)

    if "significant" not in filename:
        if num_lines != 1:
            print(f"Newsfragement {filename} can only have a single line.")
            failed = True
    else:
        # significant newsfragment
        if num_lines == 1:
            continue
        if num_lines == 2:
            print(f"Newsfragement {filename} can have 1, or 3+ lines.")
            failed = True
            continue
        if lines[1] != "":
            print(f"Newsfragement {filename} must have an empty second line.")
            failed = True
            continue

if failed:
    sys.exit(1)
