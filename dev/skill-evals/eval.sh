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
#   ./eval.sh                                          Test AGENTS.md changes
#   SKILL_NAME=airflow-contribution ./eval.sh          Test skill + AGENTS.md
#   SKILL_NAME=airflow-contribution ./eval.sh --full   Full research matrix
#   ./eval.sh --no-cache                               Disable promptfoo cache
#   ./eval.sh --repeat 3                               Run each case N times
#
# Auto-detects changes against main branch.
#   "main"    = main branch version
#   "working" = working tree version
#
# Cases test pure guidance decisions (which command to run). Arms contain
# only AGENTS.md and/or SKILL.md — no repo source code. If a future case
# needs the agent to explore real repo structure, the arm model must change.
#
# Authentication: uses Claude Code session by default (claude /login).
# Set ANTHROPIC_API_KEY to use API key auth instead.
#
# Confirmed: output_format: json_schema makes output a parsed object,
# so assertions use output.runner directly (not JSON.parse(output).runner).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
AGENTS_SRC="$REPO_ROOT/AGENTS.md"
PROMPTFOO_VERSION="0.121.17"
MODEL="${MODEL:-claude-sonnet-4-6}"

# ── Check prerequisites ──────────────────────────────────
if ! command -v node &>/dev/null; then
    echo "Error: Node.js not found. Install Node.js >=22.22.0" >&2
    exit 1
fi
if ! command -v npx &>/dev/null; then
    echo "Error: npx not found." >&2
    exit 1
fi

# Claude Agent SDK must be resolvable by promptfoo. Install once:
#   mkdir -p ~/.promptfoo-sdk && cd ~/.promptfoo-sdk && npm init -y && npm install @anthropic-ai/claude-agent-sdk
SDK_DIR="$HOME/.promptfoo-sdk/node_modules"
if [ ! -d "$SDK_DIR/@anthropic-ai/claude-agent-sdk" ]; then
    echo "Error: Claude Agent SDK not found. Run:" >&2
    echo "  mkdir -p ~/.promptfoo-sdk && cd ~/.promptfoo-sdk && npm init -y && npm install @anthropic-ai/claude-agent-sdk" >&2
    exit 1
fi

# ── Parse flags ─────────────────────────────────────────────
FULL_MODE=false
PROMPTFOO_ARGS=()
for arg in "$@"; do
    if [ "$arg" = "--full" ]; then
        FULL_MODE=true
    else
        PROMPTFOO_ARGS+=("$arg")
    fi
done

# ── Resolve skill ───────────────────────────────────────────
WITH_SKILL=false
if [ -n "${SKILL_NAME:-}" ]; then
    SKILL_SRC="$REPO_ROOT/.agents/skills/$SKILL_NAME/SKILL.md"
    if [ ! -f "$SKILL_SRC" ]; then
        echo "Error: $SKILL_SRC not found" >&2
        exit 1
    fi
    WITH_SKILL=true
fi

# ── Resolve base branch ────────────────────────────────────
BASE_BRANCH="main"
if ! git -C "$REPO_ROOT" rev-parse --verify "$BASE_BRANCH" &>/dev/null; then
    BASE_BRANCH="$(git -C "$REPO_ROOT" rev-parse --abbrev-ref HEAD)"
    echo "Warning: 'main' not found, using current branch '$BASE_BRANCH' as base"
fi

# ── Create temp dirs, clean up on exit ─────────────────────
WORK_DIR="$(mktemp -d)"
trap 'rm -rf "$WORK_DIR"' EXIT
# Symlink node_modules so promptfoo can resolve the Claude Agent SDK
# from the temp config directory (promptfoo resolves from config basePath).
ln -s "$SDK_DIR" "$WORK_DIR/node_modules"
ARMS_DIR="$WORK_DIR/arms"
mkdir -p "$ARMS_DIR"

# ── Extract "main" versions from git ──────────────────────
MAIN_AGENTS="$WORK_DIR/main-agents.md"
if ! git -C "$REPO_ROOT" show "$BASE_BRANCH:AGENTS.md" > "$MAIN_AGENTS" 2>/dev/null; then
    cp "$AGENTS_SRC" "$MAIN_AGENTS"
    echo "Warning: AGENTS.md not found on $BASE_BRANCH — main arm uses working tree copy (comparison will show no diff)"
fi

if [ "$WITH_SKILL" = true ]; then
    MAIN_SKILL="$WORK_DIR/main-skill.md"
    if ! git -C "$REPO_ROOT" show "$BASE_BRANCH:.agents/skills/$SKILL_NAME/SKILL.md" > "$MAIN_SKILL" 2>/dev/null; then
        cp "$SKILL_SRC" "$MAIN_SKILL"
        echo "Warning: SKILL.md not found on $BASE_BRANCH — main arm uses working tree copy (comparison will show no diff)"
    fi
fi

# ── Helpers ────────────────────────────────────────────────
place_skill() {
    mkdir -p "$1/.claude/skills/$SKILL_NAME"
    cp "$2" "$1/.claude/skills/$SKILL_NAME/SKILL.md"
}

place_agents() { cp "$2" "$1/AGENTS.md"; }

# ── Assemble arms ─────────────────────────────────────────
echo "Assembling arms ..."

mkdir -p "$ARMS_DIR/main" "$ARMS_DIR/working"

place_agents "$ARMS_DIR/main" "$MAIN_AGENTS"
place_agents "$ARMS_DIR/working" "$AGENTS_SRC"

if [ "$WITH_SKILL" = true ]; then
    place_skill "$ARMS_DIR/main" "$MAIN_SKILL"
    place_skill "$ARMS_DIR/working" "$SKILL_SRC"
fi

if [ "$FULL_MODE" = true ]; then
    mkdir -p "$ARMS_DIR/baseline" "$ARMS_DIR/agents-only"
    place_agents "$ARMS_DIR/agents-only" "$AGENTS_SRC"

    if [ "$WITH_SKILL" = true ]; then
        mkdir -p "$ARMS_DIR/skill-only"
        place_skill "$ARMS_DIR/skill-only" "$SKILL_SRC"
    fi
fi

# ── Generate config ───────────────────────────────────────
CONFIG="$WORK_DIR/promptfooconfig.yaml"
ARM_COUNT=0

# Output schema — inlined in every provider block (no YAML anchors,
# avoids cross-heredoc resolve issues with promptfoo's parser).
OUTPUT_SCHEMA='      output_format:
        type: json_schema
        schema:
          type: object
          required: [runner, command, rationale]
          additionalProperties: false
          properties:
            runner:
              type: string
              enum: [uv, breeze, prek]
            command:
              type: string
            rationale:
              type: string'

add_provider() {
    local label="$1"
    local dir="$2"
    local skill="$3"  # "yes" or "no"

    cat >> "$CONFIG" << EOF
  - id: anthropic:claude-agent-sdk
    label: $label
    config:
      model: $MODEL
      apiKeyRequired: false
      setting_sources: ['project']
      append_allowed_tools: ['Read', 'Grep', 'Glob']
      working_dir: $dir
EOF

    if [ "$skill" = "yes" ]; then
        echo "      skills: ['$SKILL_NAME']" >> "$CONFIG"
    fi

    echo "$OUTPUT_SCHEMA" >> "$CONFIG"
    echo "" >> "$CONFIG"

    ARM_COUNT=$((ARM_COUNT + 1))
}

# Start config — prompt template passes the request var to the agent
cat > "$CONFIG" << 'EOF'
prompts:
  - '{{request}}'

EOF
echo "providers:" >> "$CONFIG"

# Core arms
add_provider "main"    "$ARMS_DIR/main"    "$( [ "$WITH_SKILL" = true ] && echo yes || echo no )"
add_provider "working" "$ARMS_DIR/working" "$( [ "$WITH_SKILL" = true ] && echo yes || echo no )"

# Full mode arms
if [ "$FULL_MODE" = true ]; then
    add_provider "baseline"    "$ARMS_DIR/baseline"    "no"
    add_provider "agents-only" "$ARMS_DIR/agents-only" "no"
    [ "$WITH_SKILL" = true ] && add_provider "skill-only" "$ARMS_DIR/skill-only" "yes"
fi

# Default test config
if [ "$WITH_SKILL" = true ]; then
    cat >> "$CONFIG" << EOF
defaultTest:
  options:
    disableVarExpansion: true
  assert:
    - type: skill-used
      value: $SKILL_NAME

EOF
else
    cat >> "$CONFIG" << 'EOF'
defaultTest:
  options:
    disableVarExpansion: true

EOF
fi

# Cases (single source of truth)
echo "tests: file://$SCRIPT_DIR/cases/*.yaml" >> "$CONFIG"

# ── Report ────────────────────────────────────────────────
if [ "$WITH_SKILL" = true ]; then
    echo "Mode: $ARM_COUNT arms, skill '$SKILL_NAME'"
else
    echo "Mode: $ARM_COUNT arms, AGENTS.md only"
fi

echo ""
echo "Changes detected (vs $BASE_BRANCH):"
if ! diff -q "$MAIN_AGENTS" "$AGENTS_SRC" &>/dev/null; then
    echo "  AGENTS.md — modified"
else
    echo "  AGENTS.md — unchanged"
fi
if [ "$WITH_SKILL" = true ]; then
    if ! diff -q "$MAIN_SKILL" "$SKILL_SRC" &>/dev/null; then
        echo "  SKILL.md  — modified ($SKILL_NAME)"
    else
        echo "  SKILL.md  — unchanged ($SKILL_NAME)"
    fi
fi
echo ""

# ── Run ───────────────────────────────────────────────────
npx "promptfoo@$PROMPTFOO_VERSION" eval \
    -c "$CONFIG" \
    ${PROMPTFOO_ARGS[@]+"${PROMPTFOO_ARGS[@]}"}

echo ""
echo "View results: npx promptfoo@$PROMPTFOO_VERSION view"
