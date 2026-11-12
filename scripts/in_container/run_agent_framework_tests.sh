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
# uv.lock resolves. llama-index-llms-openai caps openai below 3, so Common AI's "llamaindex" extra
# is declared as conflicting with its "openai" extra and the image gets openai 3 without it. No
# workspace package depends on the OpenAI Agents SDK, so it is not locked either. Their tests are
# skipped everywhere else in CI. This installs one framework into the CI image and runs the tests
# of the framework-neutral tools and their adapters, of durable execution for Strands and the
# OpenAI Agents SDK, and of the LlamaIndex hook and operators; the other frameworks' tests skip
# themselves. One framework per invocation, so a bad release of one cannot mask the others.
#
# By default every package already in the image is held at its installed version with uv's
# --override, so the framework is tested against the same dependencies as the rest of Airflow and
# its caps on them are overridden. With --framework-pins the framework's own requirements win
# instead, which is the environment a user who installs it gets.
#
# An override replaces a requirement rather than relaxing a cap, so holding the image's openai 3
# would test LlamaIndex against an openai its packages reject, a pairing no user gets. A framework
# can therefore release named packages from the hold, and only those may change.
#
# The newest framework release older than the repository's uv exclude-newer window is installed.
set -euo pipefail

TEST_PATHS=(
    "providers/common/ai/tests/unit/common/ai/tools"
    "providers/common/ai/tests/unit/common/ai/durable/test_strands.py"
    "providers/common/ai/tests/unit/common/ai/durable/test_strands_storage.py"
    "providers/common/ai/tests/unit/common/ai/durable/test_openai_agents.py"
    "providers/common/ai/tests/unit/common/ai/hooks/test_llamaindex.py"
    "providers/common/ai/tests/unit/common/ai/operators/test_llamaindex_embedding.py"
    "providers/common/ai/tests/unit/common/ai/operators/test_llamaindex_retrieval.py"
)

cd "${AIRFLOW_SOURCES:-/opt/airflow}"

framework="${1:-}"
requirements=("${framework}")
# Image packages the framework caps below their installed version, released from the hold.
released=()
case "${framework}" in
    strands-agents) import_check="import strands" ;;
    google-adk) import_check="import google.adk" ;;
    openai-agents) import_check="import agents" ;;
    llama-index)
        mapfile -t requirements < <(python -c 'import tomllib; print("\n".join(tomllib.load(open("providers/common/ai/pyproject.toml", "rb"))["project"]["optional-dependencies"]["llamaindex"]))')
        import_check="import llama_index.core, llama_index.embeddings.openai, llama_index.llms.openai"
        released=(openai)
        ;;
    *)
        echo "Usage: $0 <strands-agents|google-adk|openai-agents|llama-index> [--framework-pins]" >&2
        exit 1
        ;;
esac
# Matches the freeze lines of the released packages; with none released it matches nothing.
released_lines="^($(IFS='|'; echo "${released[*]}"))=="

framework_pins="false"
if [[ ${2:-} == "--framework-pins" ]]; then
    framework_pins="true"
elif [[ -n ${2:-} ]]; then
    echo "Unknown argument: ${2}. The only option after the framework is --framework-pins." >&2
    exit 1
fi

if [[ ${framework_pins} == "true" ]]; then
    echo "Installing ${framework} with its own dependency pins"
    # The workspace's [tool.uv] settings shape uv.lock, not what a user who installs the framework
    # gets, so leave them out and keep only its exclude-newer window.
    exclude_newer=$(python -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["tool"]["uv"]["exclude-newer"])')
    uv pip install --no-config --exclude-newer "${exclude_newer}" "${requirements[@]}"
else
    overrides=$(mktemp)
    before=$(mktemp)
    after=$(mktemp)
    trap 'rm -f "${overrides}" "${before}" "${after}"' EXIT
    uv pip freeze | sort > "${before}"
    # Only name==version lines: editable and local installs cannot be expressed as an override,
    # and the frameworks do not depend on any of them. Overriding the rest means nothing the
    # image ships should change; the check below is there in case something still does.
    grep -E '^[A-Za-z0-9_.-]+==' "${before}" | grep -viE "${released_lines}" > "${overrides}"
    echo "Installing ${framework}, holding the image's $(wc -l < "${overrides}") installed packages"
    uv pip install --override "${overrides}" "${requirements[@]}"
    uv pip freeze | sort > "${after}"
    changed=$(comm -23 "${before}" "${after}" | grep -viE "${released_lines}" || true)
    if [[ -n ${changed} ]]; then
        echo "Installing ${framework} changed packages the image already had:" >&2
        echo "${changed}" >&2
        exit 1
    fi
fi

# Log the versions that decide whether the adapter works; the framework itself must be among them.
uv pip freeze | grep -iE '^(strands-agents|google-adk|openai-agents|llama-index-[a-z-]+|openai|mcp|opentelemetry-(api|sdk)|websockets|google-genai)==' \
    || { echo "${framework} is not installed after uv pip install" >&2; exit 1; }

# The adapter tests skip themselves when their framework is missing, so a broken install would
# otherwise pass as green.
python -c "${import_check}"

# --skip-db-tests: the job runs with backend "none", which has no database to set up.
pytest "${TEST_PATHS[@]}" --skip-db-tests -p no:cacheprovider --color=yes -ra
