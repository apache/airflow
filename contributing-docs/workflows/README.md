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

# Agent Skills for Airflow Contributors

## The Problem

Airflow has a complex contributor environment. A generic AI agent trying to help a contributor
will typically do something like:

```bash
pytest tests/models/test_dag.py
```

This fails. The correct command depends on whether you are on the host or inside Breeze, whether
the test has `@pytest.mark.db_test`, and which distribution the test belongs to. An agent reading
only flat documentation can understand this in prose but still has to reason about it — and often
gets it wrong at the exact step that matters.

---

## Two Layers

Making an agent behave like a contributor requires two complementary things:

### Layer 1 — Workflow (`SKILL.md`)

`.github/skills/airflow-contributor/SKILL.md` encodes the **full contributor workflow**: what to
do, in what order, with explicit decision points. It follows the
[quick start guide](../03a_contributors_quick_start_beginners.rst) step by step:

```
1. Determine type of change (code / docs / translation)
2. Start Breeze → verify UI at http://localhost:28080
3. Make the change
4. Run tests
   ├─ grep for db_test marker in test file
   ├─ Found → breeze run pytest (Breeze-first, never try uv)
   └─ Not found → uv run pytest (uv first, breeze fallback)
5. Run static checks (prek pre-commit, then manual before PR)
6. Build docs if .rst files changed
7. Commit → push → open PR with AI disclosure
```

An agent reading this SKILL.md knows the sequence, verification steps, and decision logic.
`skills.json` alone cannot provide this — it has commands but no sequence or decisions.

### Layer 2 — Commands (`skills.json` + `context_detect.py`)

`contributing-docs/agent_skills/skills.json` encodes **individual atomic commands** with
context-awareness. When the SKILL.md says "run tests," `context_detect.py` resolves the exact
command for the current environment:

```python
# Agent calls:
get_command("run-single-test", project="airflow-core", test_path="tests/models/test_dag.py")

# On host → returns: "uv run --project airflow-core pytest tests/models/test_dag.py -xvs"
# In Breeze → returns: "pytest tests/models/test_dag.py -xvs"
```

The two layers work together: SKILL.md tells the agent what step to take and when;
`skills.json` tells it the exact command for that step in the current environment.

---

## Why a Separate RST File, Not Existing Contributor Docs?

A fair question. The existing contributing docs (like
`03a_contributors_quick_start_beginners.rst`) are prose — narrative text, tips, warnings, and
code blocks all mixed together. There is no reliable way to automatically extract structured
metadata (id, command, fallback, context, parameters) from unstructured narrative. You'd need
NLP that understands which code block is the "main" command vs a warning vs an example — fragile
and error-prone.

The `.. agent-skill::` directive in `agent_skills.rst` is explicit structured annotation:

```rst
.. agent-skill::
   :id: run-single-test
   :local: uv run --project {project} pytest {test_path} -xvs
   :fallback: breeze run pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
```

You can parse this deterministically. The prose around it explains *why*; the directive encodes
*what*. They are complementary, not redundant.

The long-term direction (post-PoC) would be embedding these directives directly in the existing
contributing docs so the skill lives beside the prose that explains it. The separate
`agent_skills.rst` is the right scope for a PoC: one file, easy to parse, proves the concept.

---

## Key Example: `run-db-test` vs `run-single-test`

`AGENTS.md` can say "use Breeze for db tests" in prose. An agent still has to reason about
whether to try uv first. The `run-db-test` skill makes it structural:

```rst
.. agent-skill::
   :id: run-db-test
   :local: breeze run pytest {test_path} -xvs    ← no :fallback: = unconditional
   :breeze: pytest {test_path} -xvs
```

No `:fallback:` → parser generates a single unconditional host step. There is no uv step in the
JSON, so `context_detect.py` cannot return a uv command for this skill. The decision is baked
into the data, not left to reasoning.

`run-single-test` has both `:local:` and `:fallback:`, generating two host steps — try uv,
fall back to Breeze if system deps are missing. The policy is encoded, not described.

---

## How to Add a Skill

1. Add a `.. agent-skill::` block to `agent_skills.rst` alongside the prose that explains it:

   ```rst
   .. agent-skill::
      :id: my-new-skill
      :category: testing
      :description: One sentence: what it does and when to use it.
      :local: uv run --project {project} pytest {test_path} -xvs
      :fallback: breeze run pytest {test_path} -xvs
      :breeze: pytest {test_path} -xvs
      :prereqs: setup-breeze-environment
      :params: project:required,test_path:required
      :expected-output: passed
   ```

   | Option | Required | Description |
   |---|---|---|
   | `:id:` | yes | Stable slug, e.g. `run-single-test` |
   | `:category:` | yes | `environment`, `testing`, `linting`, `documentation`, `providers`, `dags` |
   | `:description:` | yes | What it does + when to use it |
   | `:local:` | yes* | Command on host machine |
   | `:fallback:` | no | Breeze fallback when `:local:` fails (triggers conditional routing) |
   | `:breeze:` | no | Command inside Breeze container |
   | `:prereqs:` | no | Comma-separated skill IDs that must run first |
   | `:params:` | no | Comma-separated `name:required` or `name:optional` pairs |
   | `:expected-output:` | no | String that signals success |

   *At least one of `:local:` or `:breeze:` is required.

2. Regenerate `skills.json`:

   ```bash
   python scripts/ci/prek/generate_agent_skills.py
   ```

3. Commit both `agent_skills.rst` and the updated `contributing-docs/agent_skills/skills.json`.

---

## Drift Detection

A prek hook (`check-agent-skills-in-sync`) re-runs the generator in `--check` mode on every
commit that touches `agent_skills.rst` or `skills.json`. If they diverge, CI fails:

```
DRIFT: skills modified in RST but skills.json not regenerated: ['run-single-test']
Run generate_agent_skills.py to regenerate.
```

Run manually:

```bash
python scripts/ci/prek/generate_agent_skills.py --check
```
