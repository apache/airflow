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
import sys
from pathlib import Path
from typing import NamedTuple

from rich.console import Console

if __name__ != "__main__":
    msg = (
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the {__file__} command"
    )
    raise SystemExit(msg)

console = Console(width=400, color_system="standard")


class RegexpSpec(NamedTuple):
    regexp: str
    replacement: str
    description: str


REPLACEMENTS: list[RegexpSpec] = [
    RegexpSpec(regexp=r"\t", replacement="    ", description="<TAB> with 4 spaces"),
    RegexpSpec(regexp=r"\u00A0", replacement=" ", description="&nbsp with space"),
    RegexpSpec(regexp=r"\u2018", replacement="'", description="left single quotation with straight one"),
    RegexpSpec(regexp=r"\u2019", replacement="'", description="right single quotation with straight one"),
    RegexpSpec(regexp=r"\u201C", replacement='"', description="left double quotation with straight one"),
    RegexpSpec(regexp=r"\u201D", replacement='"', description="right double quotation with straight one"),
]


def main() -> int:
    total_count_changes = 0
    matches = [re.compile(spec.regexp) for spec in REPLACEMENTS]
    for file_string in sys.argv:
        count_changes = 0
        path = Path(file_string)
        text = path.read_text()
        for match, spec in zip(matches, REPLACEMENTS):
            text, new_count_changes = match.subn(spec.replacement, text)
            if new_count_changes:
                console.print(
                    f"[yellow] Performed {new_count_changes} replacements of {spec.description}[/]: {path}"
                )
            count_changes += new_count_changes
        if count_changes:
            path.write_text(text)
        total_count_changes += count_changes
    return 1 if total_count_changes else 0


sys.exit(main())
