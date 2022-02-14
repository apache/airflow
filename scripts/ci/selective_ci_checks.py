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

import json
import os
import subprocess
import sys

subprocess.call("../../scripts/ci/libraries/_script_init.sh")

# Forcing PR
PR_LABELS = "full tests needed"  # os.environ.get('PR_LABELS')
GITHUB_EVENT_NAME = os.environ.get('GITHUB_EVENT_NAME')
RANDOM = os.environ.get('RANDOM')
ALL_TESTS = os.environ.get('ALL_TESTS')
INCOMING_COMMIT_SHA = os.environ.get('INCOMING_COMMIT_SHA')

if PR_LABELS == "full tests needed":
    print(f"Found the right PR labels in ${PR_LABELS} : 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = True
else:
    print(f"Did not find the right PR labels in ${PR_LABELS}: 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = False


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


def ga_output(parameter_name: str, parameter_value: str):
    print(f"::set-output name={parameter_name}::{parameter_value}")
    print(f"{parameter_name}={parameter_value}")


def output_all_basic_variables():
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = os.environ('CURRENT_PYTHON_MAJOR_MINOR_VERSIONS')
    print(f"CPMMV + {CURRENT_PYTHON_MAJOR_MINOR_VERSIONS}")
    ALL_PYTHON_MAJOR_MINOR_VERSIONS = os.environ.get('ALL_PYTHON_MAJOR_MINOR_VERSIONS')
    print(f" HERE >>  + {FULL_TESTS_NEEDED_LABEL}")
    if FULL_TESTS_NEEDED_LABEL:
        ga_output("python-versions", json.dumps(CURRENT_PYTHON_MAJOR_MINOR_VERSIONS))
        ga_output("all-python-versions", json.dumps(ALL_PYTHON_MAJOR_MINOR_VERSIONS))
        """ga_output(
            "all-python-versions",
            [
                "3.7",
                "3.8",
                "3.9",
            ],
        )"""
    else:
        ga_output(
            "python_versions",
            ["3.7"],
        )
        ga_output(
            "all-python-versions",
            ["3.7"],
        )


def set_outputs_run_everything_and_exit():
    """
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
    set_upgrade_to_newer_dependencies = upgrade_to_newer_dependencies
    needs_ui_tests = True
    needs_www_tests = True
    """
    print("needs_api_tests = True")


if len(sys.argv) < 1:
    print("")
    print("No Commit SHA - running all tests (likely direct merge, or scheduled run)!")
    print("")
    INCOMING_COMMIT_SHA = ""
    # override FULL_TESTS_NEEDED_LABEL in main/scheduled run
    FULL_TESTS_NEEDED_LABEL = True
    output_all_basic_variables()
    check_upgrade_to_newer_dependencies_needed
    set_outputs_run_everything_and_exit
else:
    INCOMING_COMMIT_SHA = sys.argv[1]
    print("")
    print(f"Commit SHA passed: {INCOMING_COMMIT_SHA}!")
    print("")


output_all_basic_variables()
