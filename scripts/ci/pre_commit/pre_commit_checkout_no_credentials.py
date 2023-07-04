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

import sys
from pathlib import Path

import yaml
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)


def check_file(the_file: Path) -> int:
    """Returns number of wrong checkout instructions in the workflow file"""
    error_num = 0
    res = yaml.safe_load(the_file.read_text())
    console.print(f"Checking file [yellow]{the_file}[/]")
    for job in res["jobs"].values():
        for step in job["steps"]:
            uses = step.get("uses")
            pretty_step = yaml.safe_dump(step, indent=2)
            if uses is not None and uses.startswith("actions/checkout"):
                with_clause = step.get("with")
                if with_clause is None:
                    console.print(f"\n[red]The `with` clause is missing in step:[/]\n\n{pretty_step}")
                    error_num += 1
                    continue
                persist_credentials = with_clause.get("persist-credentials")
                if persist_credentials is None:
                    console.print(
                        "\n[red]The `with` clause does not have persist-credentials in step:[/]"
                        f"\n\n{pretty_step}"
                    )
                    error_num += 1
                    continue
                else:
                    if persist_credentials:
                        console.print(
                            "\n[red]The `with` clause have persist-credentials=True in step:[/]"
                            f"\n\n{pretty_step}"
                        )
                        error_num += 1
                        continue
    return error_num


if __name__ == "__main__":
    total_err_num = 0
    for a_file in sys.argv[1:]:
        total_err_num += check_file(Path(a_file))
    if total_err_num:
        console.print(
            """
[red]There are are some checkout instructions in github workflows that have no "persist_credentials"
set to False.[/]

For security reasons - make sure all of the checkout actions have persist_credentials set, similar to:

  - name: "Checkout ${{ github.ref }} ( ${{ github.sha }} )"
    uses: actions/checkout@v3
    with:
      persist-credentials: false

"""
        )
        sys.exit(1)
