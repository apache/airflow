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
  - [Prerequisites](#prerequisites)
  - [Agent runtimes](#agent-runtimes)
  - [Usage](#usage)
  - [Cleanup](#cleanup)
  - [Adding cases](#adding-cases)
  - [How it works](#how-it-works)
  - [Eval-run hash gate](#eval-run-hash-gate)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Skill-Eval Harness

Test whether changes to `AGENTS.md` break or improve agent decisions.
The harness compares the `main` branch version against your working
tree, running the same cases against both and reporting the diff.

Each arm is a **shared clone** (`git clone --shared`) of the real repo in a
temp directory — the agent sees actual source files (`pyproject.toml`,
directory structure, etc.). The only difference between arms is which
`AGENTS.md` is present. Unlike `git worktree add`, a shared clone writes
nothing into the repo's own `.git`, so the eval also runs inside an agent
sandbox where `.git` is read-only.

The agent reads `AGENTS.md` directly, as Claude Code does: the pinned
Claude Agent SDK bundles a Claude Code CLI new enough (v2.1.277 or later)
to load `AGENTS.md` without a `CLAUDE.md`. Keep that in mind when changing
the SDK pin in `.pre-commit-config.yaml` — an older SDK would silently
read no guidance in any arm.

## Prerequisites

- **Claude authentication** (one of):
  - Claude Code session (`claude /login`) — Pro/Max subscription
  - `ANTHROPIC_API_KEY` environment variable — API credits
  - `SKILL_EVAL_CLAUDE_TOKEN_FILE` — path to a file holding a token from
    `claude setup-token` (Pro/Max subscription). See
    [Running from an agent sandbox](#running-from-an-agent-sandbox).
- **Codex authentication** (when using the Codex runtime): a Codex CLI
  session (`codex login`)

That's it — prek provisions Node, promptfoo, and the agent SDKs automatically.

### Running from an agent sandbox

Agent sandboxes usually block the macOS keychain and the `~/.claude`
credentials that a `/login` session stores, so every case fails with
"Not logged in".

The recommended fix is not to give the sandbox a credential at all: run the
eval yourself, outside the sandbox, with your normal login (in Claude Code,
type `! prek run run-skill-eval --hook-stage manual --all-files`). A token
file readable from the sandbox stays readable to everything that runs there
— prompt-injected commands and untrusted code under test included — for as
long as it exists.

If agents must run the eval unattended, give the eval its own credential:

```bash
claude setup-token          # run outside the agent; prints a long-lived token
# save it to a file only you can read, at a path your sandbox allows reading
chmod 600 /path/to/skill-eval-token
SKILL_EVAL_CLAUDE_TOKEN_FILE=/path/to/skill-eval-token \
  prek run run-skill-eval --hook-stage manual --all-files
```

Pick the path to suit your sandbox: sandboxes often deny reads across the
home directory (including `~/.config`), so the file must sit somewhere the
sandbox's read allowlist covers. Keep it outside the repository so it
cannot be committed by accident.

The harness reads the file and passes the token only to the promptfoo
process, so it is not exported to every command in the session — prefer
this over exporting `CLAUDE_CODE_OAUTH_TOKEN` or `ANTHROPIC_API_KEY` in the
agent's environment.

This narrows the exposure but does not hide the token from the agent: the
sandbox must allow reading the file, so anything the agent runs can read it
too. Use a token made only for the eval, and revoke it if it may have
leaked.

## Agent runtimes

Claude is the default runtime. The `run-skill-eval-codex` hook runs the same
arms and cases through the official Codex SDK instead. The Codex provider uses
a fresh thread for every prompt, read-only sandboxing, disabled network access,
disabled session-history persistence, and the SDK's structured-output support.

The two runtimes are separate hooks, not one hook with a flag, because the
Codex env carries a ~300 MB Codex CLI binary. Keeping it separate means prek
only builds that env for contributors who actually run the Codex arm. promptfoo
bundles its own older `@openai/codex-sdk`, but that build ships a binary whose
Developer ID certificate Apple has revoked — macOS kills it on exec and reports
it as malware — so the Codex hook installs a notarized version explicitly.

Codex caps the project docs it reads at `project_doc_max_bytes` and truncates
past it, and the 32,768-byte default is smaller than `AGENTS.md`. The harness
raises that cap so the whole file reaches the model — otherwise guidance
appended at the end would be invisible to every arm and the eval would compare
identical prefixes.

When `SKILL_NAME` is set, both runtimes verify skill usage with promptfoo's
`skill-used` assertion. The Codex SDK does not expose a first-class skill-use
event, so promptfoo infers usage from successful reads of the skill's
`SKILL.md` file.

```bash
# Default: Claude Agent SDK
prek run run-skill-eval --hook-stage manual --all-files

# Codex SDK, using its configured default model
prek run run-skill-eval-codex --hook-stage manual --all-files

# Codex SDK with an explicit model
MODEL=gpt-5.4 prek run run-skill-eval-codex --hook-stage manual --all-files
```

## Usage

```bash
# Run the eval (single pass over all cases — satisfies the hash gate).
# Stage your changes first — prek stashes unstaged edits:
prek run run-skill-eval --hook-stage manual --all-files

# Repeat each case to reduce nondeterminism:
EVAL_REPEAT=3 prek run run-skill-eval --hook-stage manual --all-files

# Add a baseline arm (no AGENTS.md) to measure raw model capability:
EVAL_FULL=1 prek run run-skill-eval --hook-stage manual --all-files

# Use a cheaper model for fast iteration:
MODEL=claude-haiku-4-5-20251001 prek run run-skill-eval --hook-stage manual --all-files

# Test a skill alongside AGENTS.md (not combinable with EVAL_FULL — the
# baseline arm has no skill, so the skill-used assertion would always fail).
# A skill with its own suite runs that suite (see "Skill cases"):
SKILL_NAME=airflow-pr-draft-summary prek run run-skill-eval --hook-stage manual --all-files

# View results in browser (Ctrl-C to stop the server):
prek run view-skill-eval --hook-stage manual --all-files
```

Other promptfoo flags (`--filter*`, `--no-cache`) are argv-only —
`prek run` can't forward arguments, so wire them as fixed entry args
on a hook variant if a preset is needed.

A run with `--filter*` flags covers only a subset of cases, so it does
not update the hash file.

Each run also writes a JSON report to `files/skill-evals/results.json`
(per the `files/` output convention) — handy for pasting results into
a PR.

## Cleanup

Everything the harness stores lives inside the repo — nothing is left
in your home directory:

```bash
rm -rf .build/promptfoo   # eval history and cache
prek clean                # prek-provisioned node envs
```

## Adding cases

Cases live in `cases/*.yaml`. Add entries to an existing file or
create a new one — no config changes needed.

```yaml
- description: "Scheduler bugfix (#64322): no newsfragment"
  vars:
    request: |
      I fixed the scheduler to skip asset-triggered Dags that don't
      have a SerializedDagModel yet.
      Should I create a newsfragment?
  assert:
    - type: javascript
      value: 'output.should_create === false'
```

The agent returns structured JSON (`{should_create, type, rationale}`).
Use `output.should_create` directly in assertions.

### Skill cases

A skill gets its own suite in `skills/<skill-name>/`, named after its
directory in `.agents/skills/`:

```text
skills/airflow-pr-draft-summary/
├── cases/*.yaml          # same format as above
├── output-schema.json    # JSON schema the agent's answer must follow
└── last-eval-hash.txt    # written by the run, like the top-level one
```

A skill's answers rarely fit the newsfragment schema, so each suite
brings its own, and its assertions read that schema's fields (for
example `output.title`). `SKILL_NAME=<skill-name>` runs only that
suite's cases, and every case also checks that the skill was used.
Without a suite, `SKILL_NAME` runs the `cases/*.yaml` above.

## How it works

1. Creates shared clones — one with `main`'s AGENTS.md, one with
   your working tree version. Both are full repo checkouts.
2. Generates a [promptfoo](https://github.com/promptfoo/promptfoo)
   config for the selected runtime. Claude uses promptfoo's
   `anthropic:claude-agent-sdk` provider; Codex uses its `openai:codex-sdk`
   provider. Both request schema-constrained structured output.
3. Runs each case against all arms in parallel.
4. Reports pass/fail diff. Worktrees cleaned up on exit.

## Eval-run hash gate

After every completed run, `eval.py` records a hash of its inputs
(`AGENTS.md` + `cases/*.yaml`) in `last-eval-hash.txt`. The
`check-eval-hash` prek hook — enforced locally and in CI — recomputes
the hash and fails when guidance changed without a re-run. Commit the
updated hash file together with your change.

- The hash proves the eval **ran** on this exact content, not that all
  cases passed (some cases fail even on `main` — that is signal, not a
  defect).
- WIP commits: `SKIP=check-eval-hash git commit ...` — CI stays red
  until the eval is re-run.
- Can't run the eval (no Claude subscription or API key)? Ask a
  maintainer to run it and push the updated hash file to your PR branch.
- A skill with a suite is gated the same way, by its own
  `skills/<skill-name>/last-eval-hash.txt` over its `SKILL.md`, output
  schema and cases. Changing any of them needs a
  `SKILL_NAME=<skill-name>` run. A run tests one skill, so each suite
  records its own hash, and `AGENTS.md` is left out of it: the
  top-level hash already covers it.
- Skills without a suite are not gated: with no cases, a hash over
  them would prove nothing.
- Run the eval on Linux or macOS. The hash is over file bytes, so a
  Windows checkout with CRLF line endings records a hash CI never
  matches.
