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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "pyyaml>=6.0.3",
#   "termcolor==2.5.0",
#   "wcmatch==8.2",
# ]
# ///
from __future__ import annotations

import re
import sys

from common_prek_utils import AIRFLOW_ROOT_PATH

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command"
    )


NUMBER_MATCH = re.compile(r"(^\d+\.)")


def stable_sort(x):
    return x.casefold(), x


if __name__ == "__main__":
    inthewild_path = AIRFLOW_ROOT_PATH / "INTHEWILD.md"
    content = inthewild_path.read_text()
    header = []
    companies = []
    in_header = True
    for index, line in enumerate(content.splitlines(keepends=True)):
        if in_header:
            header.append(line)
            if "Currently, **officially** using Airflow:" in line:
                in_header = False
        else:
            if line.strip() == "":
                continue
            match = NUMBER_MATCH.match(line)
            if not match:
                print(
                    f"\033[0;31mERROR: The {index + 1} line in `INTHEWILD.md` should begin with '1.'. "
                    f"Please fix it !\033[0m\n"
                )
                print(line)
                print()
                sys.exit(1)
            if not line.startswith("1."):
                print(
                    f"\033[0;33mWARNING: The {index + 1} line in `INTHEWILD.md` should begin with '1.' "
                    f"but it starts with {match.group(1)} Replacing the number with 1.\033[0m\n"
                )
                old_line = line
                line_out = "1." + line.split(".", maxsplit=1)[1]
                print(f"{old_line.strip()} => {line_out.strip()}")
            else:
                line_out = line
            companies.append(line_out)
    companies.sort(key=stable_sort)
    inthewild_path.write_text("".join(header) + "\n" + "".join(companies))
