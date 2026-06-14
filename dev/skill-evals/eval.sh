#!/bin/bash
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

# Airflow skill-eval harness
#
# Usage:
#   ./eval.sh                  Quick: 2 arms (before vs after your change)
#   ./eval.sh --full           Full:  4 arms (+ baseline, skill-only)
#   ./eval.sh --no-cache       Disable promptfoo cache
#   ./eval.sh --repeat 3       Run each case N times
#
# Auto-detects changes to AGENTS.md and SKILL.md against main branch.
# "before" = main's version, "after" = working tree.
#
# Requires: ANTHROPIC_API_KEY, npx, git

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SKILL_SRC="$REPO_ROOT/files/agent-skill-dev/airflow-contribution/SKILL.md"
AGENTS_SRC="$REPO_ROOT/AGENTS.md"
PROMPTFOO_VERSION="0.120.19"

# Parse --full flag, pass everything else to promptfoo
FULL_MODE=false
PROMPTFOO_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--full" ]; then
        FULL_MODE=true
    else
        PROMPTFOO_ARGS+=("$arg")
    fi
done

# Resolve base branch
BASE_BRANCH="main"
if ! git -C "$REPO_ROOT" rev-parse --verify "$BASE_BRANCH" &>/dev/null; then
    BASE_BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"
    echo "Warning: 'main' not found, using current branch '$BASE_BRANCH' as base"
fi

# Create temp directory, clean up on exit
ARMS_DIR="$(mktemp -d)"
trap 'rm -rf "$ARMS_DIR"' EXIT

# Helper: place skill in a working dir
place_skill() {
    local dir="$1"
    local source="$2"  # path to SKILL.md
    mkdir -p "$dir/.claude/skills/airflow-contribution"
    cp "$source" "$dir/.claude/skills/airflow-contribution/SKILL.md"
}

# Helper: place AGENTS.md in a working dir
place_agents() {
    local dir="$1"
    local source="$2"  # path to AGENTS.md
    cp "$source" "$dir/AGENTS.md"
}

# Get "before" versions from git
BEFORE_DIR="$(mktemp -d)"
trap 'rm -rf "$ARMS_DIR" "$BEFORE_DIR"' EXIT

git -C "$REPO_ROOT" show "$BASE_BRANCH:AGENTS.md" > "$BEFORE_DIR/AGENTS.md" 2>/dev/null || \
    cp "$AGENTS_SRC" "$BEFORE_DIR/AGENTS.md"

git -C "$REPO_ROOT" show "$BASE_BRANCH:files/agent-skill-dev/airflow-contribution/SKILL.md" \
    > "$BEFORE_DIR/SKILL.md" 2>/dev/null || \
    cp "$SKILL_SRC" "$BEFORE_DIR/SKILL.md"

echo "Assembling arms ..."

# === Quick mode arms (always built) ===

# before: main's AGENTS.md + main's skill
mkdir -p "$ARMS_DIR/before"
place_skill "$ARMS_DIR/before" "$BEFORE_DIR/SKILL.md"
place_agents "$ARMS_DIR/before" "$BEFORE_DIR/AGENTS.md"

# after: working tree's AGENTS.md + working tree's skill
mkdir -p "$ARMS_DIR/after"
place_skill "$ARMS_DIR/after" "$SKILL_SRC"
place_agents "$ARMS_DIR/after" "$AGENTS_SRC"

# === Full mode arms (only with --full) ===
if [ "$FULL_MODE" = true ]; then
    # baseline: no skill, no AGENTS.md
    mkdir -p "$ARMS_DIR/baseline"

    # skill-only: working tree's skill, no AGENTS.md
    mkdir -p "$ARMS_DIR/skill-only"
    place_skill "$ARMS_DIR/skill-only" "$SKILL_SRC"

    # agents-only: working tree's AGENTS.md, no skill
    mkdir -p "$ARMS_DIR/agents-only"
    place_agents "$ARMS_DIR/agents-only" "$AGENTS_SRC"

    CONFIG="$SCRIPT_DIR/promptfooconfig.full.yaml"
    echo "Mode: full (5 arms)"
else
    CONFIG="$SCRIPT_DIR/promptfooconfig.yaml"
    echo "Mode: quick (2 arms: before vs after)"
fi

# Show what changed
echo ""
echo "Changes detected (vs $BASE_BRANCH):"
if ! diff -q "$BEFORE_DIR/AGENTS.md" "$AGENTS_SRC" &>/dev/null; then
    echo "  AGENTS.md — modified"
else
    echo "  AGENTS.md — unchanged"
fi
if ! diff -q "$BEFORE_DIR/SKILL.md" "$SKILL_SRC" &>/dev/null; then
    echo "  SKILL.md  — modified"
else
    echo "  SKILL.md  — unchanged"
fi
echo ""

# Run
ARMS_DIR="$ARMS_DIR" \
    npx "promptfoo@$PROMPTFOO_VERSION" eval \
    -c "$CONFIG" \
    "${PROMPTFOO_ARGS[@]}"

echo ""
echo "View results: npx promptfoo@$PROMPTFOO_VERSION view"
