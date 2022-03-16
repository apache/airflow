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


from airflow_breeze.build_image.ci.selective_checks.utilities import (
    get_changed_files
)

INCOMING_COMMIT_SHA = os.environ.get('GITHUB_SHA')

def validate_pull_request_label(PR_LABELS):
    if "full tests needed" in PR_LABELS:
        print(f"Found the right PR labels in ${PR_LABELS} : 'full tests needed'")
        FULL_TESTS_NEEDED_LABEL = True
    else:
        print(f"Did not find the right PR labels in ${PR_LABELS}: 'full tests needed'")
        FULL_TESTS_NEEDED_LABEL = False
    return FULL_TESTS_NEEDED_LABEL


# Call it from Python Main
def validate_github_sha(GITHUB_SHA):
    PR_LABELS = os.environ.get('PR_LABELS')
    FULL_TESTS_NEEDED_LABEL = validate_pull_request_label(PR_LABELS)
    global INCOMING_COMMIT_SHA
    if not GITHUB_SHA:
        print("")
        print("No Commit SHA - running all tests (likely direct merge, or scheduled run)!")
        print("")
        INCOMING_COMMIT_SHA = ""
        FULL_TESTS_NEEDED_LABEL = "true"
        # output_all_basic_variables
        # output_all_basic_variables()
        # check_upgrade_to_newer_dependencies_needed
        # set_outputs_run_everything_and_exit
        print(FULL_TESTS_NEEDED_LABEL)
    else:
        INCOMING_COMMIT_SHA = GITHUB_SHA
        print("")
        print(f"Commit SHA passed: ${INCOMING_COMMIT_SHA}!")
        print("")


def validate_github_default_branch(default_branch):
    # === Validationg BRANCH ===
    if default_branch == "main":
        ALL_TESTS = "Always API Core Other CLI Providers WWW Integration"
        print(f'ALL_TESTS {ALL_TESTS}')
    else:
        # Skips Provider tests in case current default branch is not main
        ALL_TESTS = "Always API Core Other CLI WWW Integration"
        print(f'ALL_TESTS {ALL_TESTS}')

# Validations
def check_if_any_py_files_changed():
    """"
    Checking if Python files changed, if yes Build Image
    """
    CHANGED_FILES = get_changed_files(INCOMING_COMMIT_SHA)
    if ".py" in CHANGED_FILES:
        print("check files changed and if there is python files BUILD IMAGE")
    else:
        print("No build image")