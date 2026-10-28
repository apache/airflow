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
# uv.lock resolves; the OpenAI Agents SDK needs openai>=3 from 0.21.0, while the workspace holds
# openai below 3, so only its older releases could join the lock. Their adapter tests are skipped
# everywhere else in CI. This installs one
# framework into the CI image and runs the tests of the framework-neutral tools and their adapters,
# and of durable execution for Strands; the other frameworks' tests skip themselves. One framework
# per invocation, so a bad release of one cannot mask the others.
#
# By default every package already in the image is held at its installed version with uv's
# --override, so the framework is tested against the same dependencies as the rest of Airflow and
# its caps on them are overridden. With --framework-pins the framework's own requirements win
# instead, which is the environment a user who installs it gets.
#
# An override replaces a requirement rather than relaxing a cap, so the OpenAI Agents SDK's own
# openai>=3 would be replaced too, and its newest release would be tested against the image's
# openai 2.x, a pairing no user gets. While the image has openai 2.x, holding its packages
# therefore installs the newest release that accepts it, which is what Airflow's constraints
# resolve to.
#
# The newest framework release older than the repository's uv exclude-newer window is installed.
set -euo pipefail

TEST_PATHS=(
    "providers/common/ai/tests/unit/common/ai/tools"
    "providers/common/ai/tests/unit/common/ai/durable/test_strands.py"
    "providers/common/ai/tests/unit/common/ai/durable/test_strands_storage.py"
)

framework="${1:-}"
# What to install when the image's packages are held; with --framework-pins the newest release.
held_requirement="${framework}"
case "${framework}" in
    strands-agents) import_check="import strands" ;;
    google-adk) import_check="import google.adk" ;;
    openai-agents)
        import_check="import agents"
        if python -c 'import sys; from importlib.metadata import version; sys.exit(int(version("openai").split(".")[0]) >= 3)'; then
            held_requirement="openai-agents<0.21"
        fi
        ;;
    *)
        echo "Usage: $0 <strands-agents|google-adk|openai-agents> [--framework-pins]" >&2
        exit 1
        ;;
esac

framework_pins="false"
if [[ ${2:-} == "--framework-pins" ]]; then
    framework_pins="true"
elif [[ -n ${2:-} ]]; then
    echo "Unknown argument: ${2}. The only option after the framework is --framework-pins." >&2
    exit 1
fi

cd "${AIRFLOW_SOURCES:-/opt/airflow}"

if [[ ${framework_pins} == "true" ]]; then
    echo "Installing ${framework} with its own dependency pins"
    # The workspace's [tool.uv] overrides and constraints, such as its openai<3, shape uv.lock, not
    # what a user who installs the framework gets, so leave them out and keep only its
    # exclude-newer window.
    exclude_newer=$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["tool"]["uv"]["exclude-newer"])')
    uv pip install --no-config --exclude-newer "${exclude_newer}" "${framework}"
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
    echo "Installing ${held_requirement}, holding the image's $(wc -l < "${overrides}") installed packages"
    uv pip install --override "${overrides}" "${held_requirement}"
    uv pip freeze | sort > "${after}"
    changed=$(comm -23 "${before}" "${after}")
    if [[ -n ${changed} ]]; then
        echo "Installing ${framework} changed packages the image already had:" >&2
        echo "${changed}" >&2
        exit 1
    fi
fi

# Log the versions that decide whether the adapter works; the framework itself must be among them.
uv pip freeze | grep -iE '^(strands-agents|google-adk|openai-agents|openai|mcp|opentelemetry-(api|sdk)|websockets|google-genai)==' \
    || { echo "${framework} is not installed after uv pip install" >&2; exit 1; }

# The adapter tests skip themselves when their framework is missing, so a broken install would
# otherwise pass as green.
python -c "${import_check}"

# --skip-db-tests: the job runs with backend "none", which has no database to set up.
pytest "${TEST_PATHS[@]}" --skip-db-tests -p no:cacheprovider --color=yes -ra
