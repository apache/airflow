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

import sys
from pathlib import Path

source = sys.argv[1]
destination = sys.argv[2]
timespan_start = sys.argv[3]
timespan_end = sys.argv[4]

print(sys.argv)
print(f"Running script, called with source: {source}, destination: {destination}")
print(f"timespan_start: {timespan_start}, timespan_end: {timespan_end}")

with open(Path(destination) / "output.txt", "w+") as dest:
    for f in Path(source).glob("**/*"):
        if f.is_dir():
            continue
    with open(f) as src:
        lines = [line.upper() for line in src.readlines()]
        print(lines)
        dest.writelines(lines)
