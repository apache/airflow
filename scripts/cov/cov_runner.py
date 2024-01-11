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

import glob
import os

import pytest
from coverage import Coverage
from coverage.exceptions import NoSource


def run_tests(command_list, source, files_not_fully_covered):
    covered = sorted(
        {path for item in source for path in glob.glob(item + "/**/*.py", recursive=True)}
        - {path for path in files_not_fully_covered}
    )
    cov = Coverage(
        config_file="pyproject.toml",
        source=source,
        concurrency="multiprocessing",
    )
    with cov.collect():
        pytest.main(command_list)
    # Analyze the coverage
    failed = False
    for path in covered:
        missing_lines = cov.analysis2(path)[3]
        if len(missing_lines) > 0:
            print(f"Error: {path} has dropped in coverage. Please update tests")
            failed = True
    for path in files_not_fully_covered:
        try:
            missing_lines = cov.analysis2(path)[3]
            if not missing_lines:
                print(f"Error: {path} now has full coverage. Please remove from files_not_fully_covered")
                failed = True
        except NoSource:
            continue

    cov.html_report()
    if failed:
        print("There are some coverage errors. Please fix them")
    if len(files_not_fully_covered) > 0:
        print("Coverage run completed. Use the link below to see the coverage report")
    breeze = os.environ.get("BREEZE", "false")
    port = "8080"
    if breeze.lower() == "true":
        port = "28080"
    print(f"http://localhost:{port}/dev/coverage/index.html")
    print("You need to start the webserver before you can access the above link.")
