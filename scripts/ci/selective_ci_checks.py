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
import subprocess

PR_LABELS = os.environ.get('PR_LABELS')
GITHUB_EVENT_NAME = os.environ.get('GITHUB_EVENT_NAME')
RANDOM = os.environ.get('RANDOM')
ALL_TESTS = os.environ.get('ALL_TESTS')

if PR_LABELS == "full tests needed":
    print(f"Found the right PR labels in ${PR_LABELS} : 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = "true"
else:
    print(f"Did not find the right PR labels in ${PR_LABELS}: 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = "false"


def check_upgrade_to_newer_dependencies_needed():
    if GITHUB_EVENT_NAME == "push" or GITHUB_EVENT_NAME == "scheduled":
        # Trigger upgrading to latest constraints when we are in push or schedule event. We use the
        # random string so that it always triggers rebuilding layer in the docker image
        # Each build that upgrades to latest constraints will get truly latest constraints, not those
        # cached in the image because docker layer will get invalidated.
        # This upgrade_to_newer_dependencies variable can later be overridden
        # in case we find that any of the setup.* files changed (see below)
        global upgrade_to_newer_dependencies
        upgrade_to_newer_dependencies = RANDOM

def output_all_basic_variables():
    if FULL_TESTS_NEEDED_LABEL == True:
        NAME = "python-versions"
        subprocess.check_output(['bash', '-c', 'source _initialization.sh &&  initialization::ga_output', NAME, '${@}'])


def set_upgrade_to_newer_dependencies():
    NAME = "upgrade-to-newer-dependencies"
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = os.environ.get('CURRENT_PYTHON_MAJOR_MINOR_VERSIONS')
    subprocess.check_output(['bash', '-c', 'source _initialization.sh &&  initialization::parameters_to_json', "${CURRENT_PYTHON_MAJOR_MINOR_VERSIONS[@]}"])
    subprocess.check_output(['bash', '-c', 'source _initialization.sh &&  initialization::ga_output', 'python-versions'])

def set_outputs_run_everything_and_exit():
    needs_api_tests = True
    needs_api_codegen = True
    needs_helm_tests = True
    needs_javascript_scans = True
    needs_python_scans = True
    run_tests = True
    run_kubernetes_tests = True
    set_test_types = ALL_TESTS
    set_basic_checks_only = False
    set_docs_build = True
    set_image_build = True
    set_upgrade_to_newer_dependencies =  upgrade_to_newer_dependencies
    needs_ui_tests = True
    needs_www_tests = True

def set_outputs_run_all_python_tests():
    run_tests = True
    run_kubernetes_tests = True
    set_test_types = ALL_TESTS
    set_basic_checks_only = False
    set_image_build = True
    kubernetes_tests_needed = True

def set_output_skip_all_tests_and_docs_and_exit():
    needs_api_tests = False
    needs_api_codegen = False
    needs_helm_tests = False
    needs_javascript_scans = False
    needs_python_scans = False
    run_tests = False
    run_kubernetes_tests = False
    set_test_types = ""
    set_basic_checks_only = True
    set_docs_build = False
    set_image_build = False
    set_upgrade_to_newer_dependencies = False
    needs_ui_tests = False
    needs_www_tests = False

def set_output_skip_tests_but_build_images_and_exit():
    needs_api_tests = False
    needs_api_codegen = False
    needs_helm_tests = False
    needs_javascript_scans = False
    needs_python_scans = False
    run_tests = False
    run_kubernetes_tests = False
    set_test_types = ""
    set_basic_checks_only = False
    set_docs_build = True
    set_image_build = True
    set_upgrade_to_newer_dependencies = upgrade_to_newer_dependencies
    needs_ui_tests = False
    needs_www_tests = False

def get_regexp_from_patterns():
    """
    Converts array of patterns into single | pattern string
    pattern_array - array storing regexp patterns
    Outputs - pattern string
    """
    test_triggering_regexp = ""
    separator = ""
    pattern_array = os.environ.get('pattern_array')
    for pattern in pattern_array:
        test_triggering_regexp += separator + pattern
        separator = "|"
    print(test_triggering_regexp)

def show_changed_files():
    """
    # Shows changed files in the commit vs. the target.
    # Input:
    #    pattern_array - array storing regexp patterns
    """
    CHANGED_FILES = os.environ.get('CHANGED_FILES')    
    THE_REGEXP = get_regexp_from_patterns()
    print("")
    print(f"Changed files matching the ${THE_REGEXP} pattern:")
    print("")
    for file in CHANGED_FILES:
        if file in  THE_REGEXP:
            print(f"{CHANGED_FILES}")
        else:
            return True
    print("")

def count_changed_files():
    """
    Counts changed files in the commit vs. the target
    Input:
        pattern_array - array storing regexp patterns
    Output:
        Count of changed files matching the patterns
    """
    CHANGED_FILES = os.environ.get('CHANGED_FILES')    

    for file in CHANGED_FILES:
        if file in get_regexp_from_patterns():
            print(file)
        else:
            return True

def check_if_python_security_scans_should_be_run():
    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_start', "Check Python security scans"])
    pattern_array=["^airflow/.*\.py", "^setup.py"]
    show_changed_files

    if count_changed_files == "0":
        needs_python_scans = False
    else:
        needs_python_scans = True
    
    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_end'])

def check_if_setup_files_changed():
    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_start', "Check setup.py/cfg changed"])
    pattern_array = ["^setup.cfg", "^setup.py"]

    show_changed_files

    if count_changed_files != "0":
        upgrade_to_newer_dependencies="${RANDOM}"

    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_end'])

def check_if_javascript_security_scans_should_be_run():
    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_start', "Check JavaScript security scans"])
    pattern_array=["^airflow/.*\.[jt]sx?", "^airflow/.*\.lock"]

    show_changed_files

    if count_changed_files == "0":
        needs_javascript_scans = False
    else:
        needs_javascript_scans = True
    
    subprocess.check_output(['bash', '-c', 'source _start_end.sh &&  start_end::group_end'])