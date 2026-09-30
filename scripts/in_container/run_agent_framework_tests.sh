#!/usr/bin/env bash
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

# Runs the common.ai adapter tests for agent frameworks that cannot join the workspace lock.
#
# Strands Agents caps mcp, and Google ADK caps opentelemetry and websockets, below the versions
# uv.lock resolves, so their adapter tests are skipped everywhere else in CI. This installs the
# frameworks into the CI image and runs the tests of the framework-neutral tools and their adapters.
#
# By default every package already in the image is held at its installed version with uv's
# --override, so the framework is tested against the same dependencies as the rest of Airflow and
# its caps on them are overridden. With --framework-pins the framework's own requirements win
# instead, which is the environment a user who installs it gets.
#
# The newest framework releases older than the repository's uv exclude-newer window is installed.
set -euo pipefail

FRAMEWORKS=("strands-agents" "google-adk")
TEST_PATH="providers/common/ai/tests/unit/common/ai/tools"

framework_pins="false"
if [[ ${1:-} == "--framework-pins" ]]; then
    framework_pins="true"
elif [[ -n ${1:-} ]]; then
    echo "Unknown argument: ${1}. The only option is --framework-pins." >&2
    exit 1
fi

cd "${AIRFLOW_SOURCES:-/opt/airflow}"

if [[ ${framework_pins} == "true" ]]; then
    echo "Installing ${FRAMEWORKS[*]} with their own dependency pins"
    uv pip install "${FRAMEWORKS[@]}"
else
    overrides=$(mktemp)
    before=$(mktemp)
    after=$(mktemp)
    trap 'rm -f "${overrides}" "${before}" "${after}"' EXIT
    uv pip freeze | sort > "${before}"
    # Only name==version lines: editable and local installs cannot be expressed as an override,
    # and the frameworks do not depend on any of them. Overriding the rest means nothing the
    # image ships should change; the check below is there in case something still does.
    grep -E '^[A-Za-z0-9_.-]+==' "${before}" > "${overrides}"
    echo "Installing ${FRAMEWORKS[*]}, holding the image's $(wc -l < "${overrides}") installed packages"
    uv pip install --override "${overrides}" "${FRAMEWORKS[@]}"
    uv pip freeze | sort > "${after}"
    changed=$(comm -23 "${before}" "${after}")
    if [[ -n ${changed} ]]; then
        echo "Installing the frameworks changed packages the image already had:" >&2
        echo "${changed}" >&2
        exit 1
    fi
fi

uv pip freeze | grep -iE '^(strands-agents|google-adk|mcp|opentelemetry-(api|sdk)|websockets|google-genai)=='

# The adapter tests skip themselves when their framework is missing, so a broken install would
# otherwise pass as green.
python -c "import strands, google.adk"

# --skip-db-tests: the job runs with backend "none", which has no database to set up.
pytest "${TEST_PATH}" --skip-db-tests -p no:cacheprovider --color=yes -ra
