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
import glob
import os
import re
import sys
from os.path import abspath, dirname, join

AIRFLOW_SOURCES_DIR = abspath(join(dirname(__file__), os.pardir, os.pardir, os.pardir))

sys.path.insert(0, AIRFLOW_SOURCES_DIR)
# flake8: noqa: F401

from setup import version  # isort:skip


def update_version(pattern, v: str, file_path: str):
    print(f"Replacing {pattern} to {version} in {file_path}")
    with open(file_path, "r+") as f:
        file_contents = f.read()
        lines = file_contents.splitlines(keepends=True)
        for i in range(0, len(lines)):
            lines[i] = re.sub(pattern, fr'\g<1>{v}\g<2>', lines[i])
        file_contents = "".join(lines)
        f.seek(0)
        f.truncate()
        f.write(file_contents)


REPLACEMENTS = {
    r'(FROM apache/airflow:).*($)': "docs/docker-stack/docker-examples/extending/*/Dockerfile",
    r'(apache/airflow:)[^-]*(\-)': "docs/docker-stack/entrypoint.rst",
    r'(/constraints-)[^-]*(/constraints)': "docs/docker-stack/docker-examples/"
    "restricted/restricted_environments.sh",
    r'(AIRFLOW_VERSION=")[^"]*(" \\)': "docs/docker-stack/docker-examples/"
    "restricted/restricted_environments.sh",
}

if __name__ == '__main__':
    for regexp, p in REPLACEMENTS.items():
        text_pattern = re.compile(regexp)
        files = glob.glob(join(AIRFLOW_SOURCES_DIR, p), recursive=True)
        if not files:
            print(f"ERROR! No files matched on {p}")
        for file in glob.glob(join(AIRFLOW_SOURCES_DIR, p), recursive=True):
            update_version(text_pattern, version, file)
