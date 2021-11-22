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
Test for an order of dependencies in setup.py
"""
import difflib
import os
import sys
import textwrap
from typing import List

from rich import print

errors = []

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir, os.pardir, os.pardir))
BUILD_ARGS_REF_PATH = os.path.join(SOURCE_DIR_PATH, "docs", "docker-stack", "build-arg-ref.rst")

START_LINE = ".. BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT"
END_LINE = ".. END OF EXTRAS LIST UPDATED BY PRE COMMIT"


class ConsoleDiff(difflib.Differ):
    def _dump(self, tag, x, lo, hi):
        """Generate comparison results for a same-tagged range."""
        for i in range(lo, hi):
            if tag == "+":
                yield f'[green]{tag} {x[i]}[/]'
            elif tag == "-":
                yield f'[red]{tag} {x[i]}[/]'
            else:
                yield f'{tag} {x[i]}'


def _check_list_sorted(the_list: List[str], message: str) -> bool:
    sorted_list = sorted(the_list)
    if the_list == sorted_list:
        print(f"{message} is [green]ok[/]")
        print(the_list)
        print()
        return True
    print(f"{message} [red]NOK[/]")
    print(textwrap.indent("\n".join(ConsoleDiff().compare(the_list, sorted_list)), " " * 4))
    print()
    errors.append(f"ERROR in {message}. The elements are not sorted.")
    return False


def check_dockerfile():
    with open(os.path.join(SOURCE_DIR_PATH, "Dockerfile")) as dockerfile:
        file_contents = dockerfile.read()
    extras_list = None
    for line in file_contents.splitlines():
        if line.startswith("ARG AIRFLOW_EXTRAS="):
            extras_list = line.split("=")[1].replace('"', '').split(",")
            if _check_list_sorted(extras_list, "Dockerfile's AIRFLOW_EXTRAS"):
                with open(BUILD_ARGS_REF_PATH) as build_args_file:
                    content = build_args_file.read().splitlines(keepends=False)
                result = []
                is_copying = True
                for line in content:
                    if line.startswith(START_LINE):
                        result.append(f"{line}\n")
                        is_copying = False
                        for extra in extras_list:
                            result.append(f'* {extra}')
                    elif line.startswith(END_LINE):
                        result.append(f"\n{line}")
                        is_copying = True
                    elif is_copying:
                        result.append(line)
                with open(BUILD_ARGS_REF_PATH, "w") as build_args_file:
                    build_args_file.write("\n".join(result))
                    build_args_file.write("\n")
                return
    if not extras_list:
        errors.append("Something is wrong. Dockerfile does not contain AIRFLOW_EXTRAS")


if __name__ == '__main__':
    check_dockerfile()
    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
