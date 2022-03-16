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
import os.path

import click

from airflow_breeze.build_image.ci.selective_checks.utilities import get_changed_files
from airflow_breeze.build_image.ci.selective_checks.validate_selective_checks import (
    check_if_any_py_files_changed,
    validate_github_default_branch,
    validate_github_sha,
)

GITHUB_SHA = os.environ.get('GITHUB_SHA')
DEFAULT_BRANCH = os.environ.get('DEFAULT_BRANCH')
PR_LABELS = os.environ.get('PR_LABELS')
INCOMING_COMMIT_SHA = os.environ.get('GITHUB_SHA')


ANY_PY_FILES_CHANGED = r"\.py$"
AIRFLOW_SOURCES_TRIGGERING_TESTS = """    '^.pre-commit-config.yaml$'
    '^airflow'
    '^chart'
    '^tests'
    '^kubernetes_tests'"""
upgrade_to_newer_dependencies = "false"
print(AIRFLOW_SOURCES_TRIGGERING_TESTS)
print(ANY_PY_FILES_CHANGED)


@click.group()
def main():
    validate_github_sha(GITHUB_SHA)
    validate_github_default_branch(DEFAULT_BRANCH)
    get_changed_files(INCOMING_COMMIT_SHA)
    check_if_any_py_files_changed()


@main.command(name='build-image')
def build_image():
    print("BUILD IMAGE")


@main.command(name='matrix-strategy')
def matrix_strategy():
    print("PRINT python-versions")


if __name__ == '__main__':
    main()
