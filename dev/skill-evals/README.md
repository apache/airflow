<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Skill-Eval Harness](#skill-eval-harness)
  - [Scope and limitations](#scope-and-limitations)
  - [Prerequisites](#prerequisites)
  - [One-time setup](#one-time-setup)
  - [Quick start](#quick-start)
  - [When to use which mode](#when-to-use-which-mode)
  - [Usage](#usage)
  - [Adding cases](#adding-cases)
  - [How it works](#how-it-works)
  - [Directory structure](#directory-structure)
  - [Prek hook](#prek-hook)
  - [Version pinning](#version-pinning)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->
 TODO: This license is not consistent with the license used in the project.
       Delete the inconsistent license and above line and rerun pre-commit to insert a good license.

<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

# Skill-Eval Harness

A dev-time tool for verifying that changes to `AGENTS.md` or skill files
(`SKILL.md`) improve — or at least don't regress — agent behavior.

The harness compares the `main` branch version against your working tree,
running the same probe cases against both and reporting the diff. Config
is generated at runtime — there are no static config files that can drift.

## Scope and limitations

Cases test **pure guidance decisions** — "given this scenario, which
command should the agent choose?" Arms contain only `AGENTS.md` and/or
`SKILL.md` in temporary working directories. The agent does not see
the real repository structure.

This means the harness is suitable for:

- Command routing decisions (uv / breeze / prek)
- Static check selection
- Guidance quality regression testing

It is **not** suitable for cases that require the agent to explore
actual source files, read test markers, or inspect `pyproject.toml`.
If a future case needs real repo context, the arm model must change.

## Prerequisites

- **Node.js >=22.22.0** — check with `node --version`, upgrade with
  `nvm install 22.22.0` if needed
- **`npx`** (comes with Node.js)
- **`git`** (used to extract `main` branch versions for comparison)
- **Authentication** (one of):
  - Claude Code session (`claude /login`) — works with Pro/Max subscription
  - `ANTHROPIC_API_KEY` environment variable — works with API credits

## One-time setup

Install the Claude Agent SDK in a dedicated directory (promptfoo
needs it at runtime but does not bundle it):

```bash
mkdir -p ~/.promptfoo-sdk && cd ~/.promptfoo-sdk && npm install @anthropic-ai/claude-agent-sdk
```

Log in to Claude Code (if using Pro/Max subscription):

```bash
claude /login
```

## Quick start

```bash
# Modified AGENTS.md and want to check for regressions:
./dev/skill-evals/eval.sh

# Modified a skill and want to check for regressions vs main:
SKILL_NAME=airflow-contribution ./dev/skill-evals/eval.sh
```

Output:

```
Assembling arms ...
Mode: 2 arms, AGENTS.md only

Changes detected (vs main):
  AGENTS.md — modified

         | main   | working
case-1   | PASS   | PASS      ← no impact
case-2   | PASS   | FAIL      ← regression
case-3   | FAIL   | PASS      ← improvement
```

## When to use which mode

**Quick mode (default):** regression testing. You changed `AGENTS.md`
or `SKILL.md` and want to know "did my change make things better or
worse compared to main?" Two arms: `main` vs `working`. Fast, cheap,
answers the daily question. When used with `SKILL_NAME`, both arms
contain the skill *and* `AGENTS.md` — it tells you whether your skill
edit regressed, not whether the skill itself is effective.

**Full mode (`--full`):** effectiveness analysis. You want to understand
*why* something passes or fails — is it the skill helping, the
`AGENTS.md` guiding, or the model already knowing? Adds isolation arms
(`baseline`, `skill-only`, `agents-only`) that separate each component's
contribution. Use periodically, not on every change.

## Usage

```bash
# AGENTS.md only — no skill involved
./dev/skill-evals/eval.sh

# Skill + AGENTS.md
SKILL_NAME=airflow-contribution ./dev/skill-evals/eval.sh

# Full research matrix (adds baseline, skill-only, agents-only)
SKILL_NAME=airflow-contribution ./dev/skill-evals/eval.sh --full

# Use a cheaper model for fast iteration
MODEL=claude-haiku-4-5-20251001 ./dev/skill-evals/eval.sh

# Repeat each case to account for nondeterminism (recommended: 3)
./dev/skill-evals/eval.sh --repeat 3

# Disable cache (force fresh LLM calls)
./dev/skill-evals/eval.sh --no-cache

# View results in browser
npx promptfoo@0.121.17 view
```

### Environment variables

| Variable | Required | Default | Description |
|----------|:--------:|---------|-------------|
| `ANTHROPIC_API_KEY` | no | — | Anthropic API key. If unset, authenticates via Claude Code session (`claude /login`) |
| `SKILL_NAME` | no | — | Skill to test (e.g. `airflow-contribution`). Omit to test AGENTS.md only |
| `MODEL` | no | `claude-sonnet-4-6` | Model to use for eval |

### Arms by mode

| Arm | Quick | Quick + skill | Full | Full + skill |
|-----|:-----:|:-------------:|:----:|:------------:|
| `main` | yes | yes | yes | yes |
| `working` | yes | yes | yes | yes |
| `baseline` | — | — | yes | yes |
| `agents-only` | — | — | yes | yes |
| `skill-only` | — | — | — | yes |

`main` and `working` always contain `AGENTS.md`. When `SKILL_NAME` is
set, they also contain the skill. Isolation arms (`baseline`,
`agents-only`, `skill-only`) contain only the component named — no
other guidance files.

`baseline` is an empty working directory. The agent receives no
`AGENTS.md` and no skill — this measures raw model capability on the
probe. Since arms contain only guidance files (no repo source code),
`baseline` tests what the model knows without any project-specific
instructions.

## Adding cases

Cases live in `cases/*.yaml`. Add entries to an existing file or create
a new one — no config changes required, no directory structure to learn.

```yaml
# cases/command-routing.yaml
- description: "Provider test: uv fails, fallback to breeze"
  vars:
    request: |
      Run amazon provider tests.
      uv failed with: error: libpq-dev not found
  assert:
    - type: javascript
      value: 'output.runner === "breeze"'
```

Every case runs against all arms automatically. Adding a case always
means editing exactly one file.

The Agent SDK provider with `output_format: json_schema` returns
`output` as a parsed object — use `output.runner` directly, not
`JSON.parse(output).runner`.

## How it works

1. `eval.sh` extracts the `main` branch versions of `AGENTS.md` (and
   `SKILL.md` if `SKILL_NAME` is set) via `git show`. If `main` does
   not have the file, it falls back to the working tree copy and prints
   a warning — the comparison will show no diff in that case.
2. It builds temporary working directories — one per arm — placing
   files where the Claude Agent SDK discovers them (`.claude/skills/`
   for skills, root for `AGENTS.md`).
3. It generates a promptfoo config into the temp directory. The output
   schema is inlined in every provider block (no YAML anchors — avoids
   cross-heredoc resolve issues with promptfoo's parser).
4. promptfoo runs each case against all arms in parallel via the
   `anthropic:claude-agent-sdk` provider, which provides:
   - `output_format: json_schema` — structured output without
     markdown fence stripping
   - `skill-used` assertion — confirms the agent actually invoked
     the skill, not just happened to answer correctly
5. Temporary directories and config are cleaned up on exit.

## Directory structure

```
dev/skill-evals/
  eval.sh           # entry point — assembles arms, generates config, runs eval
  README.md         # this file
  cases/
    command-routing.yaml   # probe cases (uv / breeze / prek routing)
```

No static config files. The promptfoo config is generated into a temp
directory at runtime, based on `SKILL_NAME` and `--full` flags.

## Prek hook

A prek hook (`check-eval-reminder`) prints a reminder when `AGENTS.md`
or `SKILL.md` is staged for commit:

```
⚠  AGENTS.md modified — consider running ./dev/skill-evals/eval.sh before pushing
```

The hook never blocks the commit — it is informational only.

## Version pinning

The harness pins promptfoo to a specific version (`0.121.17`) to ensure
reproducible results. Update the version in `eval.sh` when upgrading.

promptfoo is owned by OpenAI (acquired 2026-03). The harness treats it
as an execution convenience layer — the investment is in test cases
(YAML), not runner infrastructure. Cases are portable and can be run
with a simple Python script if promptfoo becomes unusable.
