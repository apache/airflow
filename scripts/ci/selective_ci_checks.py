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

PR_LABELS = os.environ.get('PR_LABELS')
GITHUB_EVENT_NAME = os.environ.get('GITHUB_EVENT_NAME')


if os.environ.get("PR_LABELS") == "full tests needed":
    print(f"Found the right PR labels in ${PR_LABELS} : 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = "true"
else:
    print(f"Did not find the right PR labels in ${PR_LABELS}: 'full tests needed'")
    FULL_TESTS_NEEDED_LABEL = "false"


# def check_upgrade_to_newer_dependencies_needed():
#     RANDOM = os.environ.get('RANDOM')
#     if GITHUB_EVENT_NAME == "push" or GITHUB_EVENT_NAME == "scheduled":
#         # Trigger upgrading to latest constraints when we are in push or schedule event. We use the
#         # random string so that it always triggers rebuilding layer in the docker image
#         # Each build that upgrades to latest constraints will get truly latest constraints, not those
#         # cached in the image because docker layer will get invalidated.
#         # This upgrade_to_newer_dependencies variable can later be overridden
#         # in case we find that any of the setup.* files changed (see below)
#         upgrade_to_newer_dependencies = RANDOM
