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
import subprocess

INCOMING_COMMIT_SHA = os.environ.get('GITHUB_SHA')


def get_changed_files(INCOMING_COMMIT_SHA):
    print("")
    print(f"Incoming commit SHA: ${INCOMING_COMMIT_SHA}")
    print("")
    print(f"Changed files from ${INCOMING_COMMIT_SHA} vs it's first parent")
    print("")
    CHANGED_FILES = (
        subprocess.check_output(
            [
                "git",
                "diff-tree",
                "--no-commit-id",
                "--name-only",
                "-r",
                f"{INCOMING_COMMIT_SHA}^",
                f"{INCOMING_COMMIT_SHA}",
            ]
        )
        .decode('utf-8')
        .strip()
    )

    if not CHANGED_FILES:
        print("")
        # print(f"${COLOR_YELLOW}WARNING: Could not find any changed files  ${COLOR_RESET}"
        print("WARNING: Could not find any changed files")
        print("Assuming that we should run all tests in this case")
        print("")
        print("set_outputs_run_everything_and_exit")
    else:
        print("")
        print("Changed files:")
        print("")
        print(CHANGED_FILES)

    return CHANGED_FILES
