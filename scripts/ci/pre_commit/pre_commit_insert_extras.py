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
import os
import sys
from os.path import dirname
from textwrap import wrap
from typing import List

AIRFLOW_SOURCES_DIR = os.path.join(dirname(__file__), os.pardir, os.pardir, os.pardir)

sys.path.insert(0, AIRFLOW_SOURCES_DIR)
# flake8: noqa: F401
# pylint: disable=wrong-import-position
from setup import EXTRAS_REQUIREMENTS  # isort:skip

sys.path.append(AIRFLOW_SOURCES_DIR)

RST_HEADER = '  .. START EXTRAS HERE'
RST_FOOTER = '  .. END EXTRAS HERE'

INSTALL_HEADER = '# START EXTRAS HERE'
INSTALL_FOOTER = '# END EXTRAS HERE'


def insert_documentation(file_path: str, content: List[str], header: str, footer: str):
    with open(file_path, "r") as documentation_file:
        replacing = False
        result: List[str] = []
        text = documentation_file.readlines()
        for line in text:
            if line.startswith(header):
                replacing = True
                result.append(line)
                result.append("\n")
                result += content
                result.append("\n")
            if line.startswith(footer):
                replacing = False
            if not replacing:
                result.append(line)
    with open(file_path, "w") as documentation_file:
        documentation_file.write("".join(result))


if __name__ == '__main__':
    install_file_path = os.path.join(AIRFLOW_SOURCES_DIR, 'INSTALL')
    contributing_file_path = os.path.join(AIRFLOW_SOURCES_DIR, 'CONTRIBUTING.rst')
    extras = wrap(", ".join(EXTRAS_REQUIREMENTS.keys()), 100)
    extras = [line + "\n" for line in extras]
    insert_documentation(install_file_path, extras, INSTALL_HEADER, INSTALL_FOOTER)
    insert_documentation(contributing_file_path, extras, RST_HEADER, RST_FOOTER)
