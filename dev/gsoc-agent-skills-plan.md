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

- [GSoC Agent Skills for Apache Airflow — Implementation Plan](#gsoc-agent-skills-for-apache-airflow--implementation-plan)
  - [The Core Problem](#the-core-problem)
  - [What "Agent Skills" Are (and aren't)](#what-agent-skills-are-and-arent)
  - [Proposed Architecture: RST-First + SKILL.md Generation](#proposed-architecture-rst-first--skillmd-generation)
  - [RST Directive Design](#rst-directive-design)
  - [SKILL.md Design (agentskills.io Format)](#skillmd-design-agentskillsio-format)
  - [`extract_agent_skills.py` — Core Script](#extract_agent_skillspy--core-script)
  - [`context_detect.py` Additions](#context_detectpy-additions)
  - [CI/prek Hook](#ciprek-hook)
  - [Verification Strategy](#verification-strategy)
  - [Migration Phases](#migration-phases)
  - [File Summary](#file-summary)
  - [Answers to Mentor's Questions](#answers-to-mentors-questions)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# GSoC Agent Skills for Apache Airflow — Implementation Plan

## The Core Problem

**Goal:** Make AI agents behave like experienced contributors — run the correct command for each task, in the correct environment, without guessing.

**Current PoC gap:** YAML workflow files in `contributing-docs/workflows/*.yaml` are a *new* parallel representation of knowledge that already exists in the RST contributor docs. Two places to maintain the same truth → inevitable drift. Maintainers must learn YAML schema on top of RST authoring.

**Mentor's key constraint:** Contributor docs (RST) must be the single source of truth for agent skills.

---

## What "Agent Skills" Are (and aren't)

**agentskills.io SKILL.md format** — files auto-discovered by Claude Code from `.github/skills/*/SKILL.md`. Each has:

- YAML frontmatter: `name:` (slug), `description:` (what it does + when to use it — critical for skill selection)
- Markdown body: concise workflow instructions with decision logic

**Not AGENTS.md:** grows monolithic, no progressive disclosure, agent reads all of it every time.

**Not MCP:** requires running server infrastructure. SKILL.md files are zero-infrastructure — just committed markdown.

**Not YAML→JSON pipeline:** disconnected from human-maintained docs, maintenance burden doubles.

---

## Proposed Architecture: RST-First + SKILL.md Generation

```
contributing-docs/*.rst               ← SINGLE SOURCE OF TRUTH
  └── .. agent-skill:: directives     ← embedded in existing narrative prose
          │
          ▼
scripts/ci/prek/extract_agent_skills.py   (replaces generate_agent_skills.py)
          │
          ├──▶ .github/skills/{id}/SKILL.md     ← Claude Code auto-discovery
          └──▶ contributing-docs/agent_skills/skills.json   ← format-agnostic API
                    │
                    ▼
          context_detect.py           ← runtime command routing (unchanged API)
```

**Why this works for maintainability:**

- Contributors edit RST docs exactly as they do today
- `.. agent-skill::` directive sits directly beside the prose that explains it
- prek hook catches any drift between RST directives and generated SKILL.md files
- No separate YAML to maintain

---

## RST Directive Design

Embed `.. agent-skill::` directives in **existing** RST files, not a new standalone file:

```rst
.. agent-skill::
   :id: run-single-test
   :category: testing
   :description: Run a targeted pytest test locally with uv, falling back to Breeze if system deps are missing. Use when asked to run a specific test. Check for @pytest.mark.db_test first — use run-db-test skill if present.
   :local: uv run --project {project} pytest {test_path} -xvs
   :fallback: breeze run pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
   :prereqs: setup-breeze-environment
   :params: project:required,test_path:required
   :expected-output: passed
```

### Where directives go (RST file → skill)

| Skill ID | RST File |
|---|---|
| `setup-breeze-environment` | `contributing-docs/06_development_environments.rst` |
| `run-single-test` | `contributing-docs/testing/unit_tests.rst` |
| `run-db-test` | `contributing-docs/testing/unit_tests.rst` (after db_test section) |
| `run-static-checks` | `contributing-docs/08_static_code_checks.rst` |
| `run-manual-checks` | `contributing-docs/08_static_code_checks.rst` |
| `build-docs` | `contributing-docs/11_documentation_building.rst` |
| `run-provider-tests` | `contributing-docs/12_provider_distributions.rst` |

The existing `contributing-docs/workflows/agent_skills.rst` (comparison artifact) and `contributing-docs/workflows/*.yaml` (5 YAML files) are deprecated once migration is complete.

---

## SKILL.md Design (agentskills.io Format)

Following the existing `.github/skills/airflow-translations/SKILL.md` as the model.

### Key principles from agentskills.io

- **Concise is key** — Claude is already smart; only add what it doesn't know
- **Description = discovery trigger** — must say BOTH what it does AND when to use it
- **Workflow checklists** for multi-step tasks
- **Feedback loops** — validate → fix → repeat
- **Low freedom where fragile** (exact commands); **high freedom** for general reasoning

### New skill: `run-db-test` (critical addition)

The current PoC is missing this. The mentor's Slack discussion highlights it: `@pytest.mark.db_test` tests need Breeze **directly** (not as fallback). This skill encodes the "Breeze-first, not Breeze-fallback" rule:

````text
---
name: run-db-test
description: >
  Run Airflow tests marked with @pytest.mark.db_test. These MUST use Breeze
  directly - do NOT try uv first. Use when a test file, class, or module
  contains @pytest.mark.db_test or pytestmark = pytest.mark.db_test.
license: Apache-2.0
---

# Why Breeze-First (Not Fallback)

DB tests require a live Airflow metadata database. uv run on the host does
not provision one. Go directly to Breeze - no local attempt first.

# Detect DB Tests

Run: grep -n "db_test" {test_path}

If this returns results - use this skill. If not - use run-single-test.

# Commands

Single test (from host):
  breeze run pytest {test_path} -xvs

Full db suite:
  breeze testing core-tests --run-db-tests-only --run-in-parallel

With Postgres backend:
  breeze testing core-tests --run-db-tests-only --backend postgres --run-in-parallel

Already inside Breeze:
  pytest {test_path} -xvs

# Success Signal

Output contains "passed".
````

### `run-single-test` SKILL.md (revised with decision logic)

````text
---
name: run-single-test
description: >
  Run a targeted pytest test for a changed Airflow module or provider. Use
  when asked to run, execute, or verify a specific test file or method. First
  check for @pytest.mark.db_test - if present, use run-db-test skill instead.
license: Apache-2.0
---

# Decision: Which Environment?

1. Check for db_test marker: grep -n "db_test" {test_path}
   - Found: Use run-db-test skill (Breeze required, skip local attempt).
   - Not found: Continue below.

2. Try local first (faster, IDE-debuggable):
     uv run --project {project} pytest {test_path} -xvs

3. If uv fails with missing system dependencies, fall back to Breeze:
     breeze run pytest {test_path} -xvs

4. If already inside Breeze:
     pytest {test_path} -xvs

# Parameters

- {project}: folder with pyproject.toml, e.g. airflow-core or providers/amazon
- {test_path}: test file path, e.g. airflow-core/tests/models/test_dag.py::TestDag::test_method

# Success Signal

Output contains "passed".
````

---

## `extract_agent_skills.py` — Core Script

**File:** `scripts/ci/prek/extract_agent_skills.py`
**Replaces:** `generate_agent_skills.py` (YAML-based)

### Modes

- `python extract_agent_skills.py` — generate SKILL.md files + JSON from RST
- `python extract_agent_skills.py --check` — diff re-generated vs committed, exit 1 on drift (CI mode)

### Key functions

```python
def parse_rst_for_skills(docs_dir: Path) -> list[SkillData]:
    """Walk contributing-docs/*.rst recursively, extract .. agent-skill:: blocks."""


def extract_skill_from_directive(lines: list[str], source_file: Path) -> SkillData:
    """Parse a single directive block into SkillData."""


def render_skill_md(skill: SkillData) -> str:
    """Generate SKILL.md content from SkillData (decision logic + commands)."""


def check_drift(docs_dir, skills_dir, output_dir) -> int:
    """Re-generate in memory, diff vs committed files. Return 1 if drifted."""
```

### SkillData dataclass

```python
@dataclass
class SkillData:
    id: str
    category: str
    description: str  # must include trigger condition
    local: str | None  # :local: → uv run / host command
    fallback: str | None  # :fallback: → breeze run (when local fails)
    breeze: str | None  # :breeze: → command inside Breeze container
    prereqs: list[str]
    params: dict[str, bool]  # {name: required}
    expected_output: str | None
    source_file: str  # which RST file contains this skill
    source_line: int
```

### Reuse from current codebase

- Cycle detection (`detect_cycle`) — copy from `generate_agent_skills.py`
- Prereq validation (`validate_prereq_references`) — copy unchanged
- JSON schema for `skills.json` — unchanged (backward-compatible with `context_detect.py`)

---


## `context_detect.py` Additions

Three new functions to encode the Breeze-first routing rule programmatically:

```python
def has_db_test_marker(test_path: str | Path) -> bool:
    """Return True if test file contains @pytest.mark.db_test at any level.
    Uses regex (no AST) — cheap, no heavy imports.
    """


def get_skill_for_test(test_path: str | Path) -> str:
    """Return 'run-db-test' if db_test marker present, 'run-single-test' otherwise."""


def get_command_for_test(test_path, project, **kwargs) -> str:
    """Convenience: get_command(get_skill_for_test(test_path), ...)"""
```

CLI addition:

```bash
python context_detect.py --test-for path/to/test.py
# → Outputs: skill=run-db-test, command=breeze run pytest ...
```

**All existing APIs unchanged** (`get_context()`, `get_command()`, `list_skills_for_context()`).

---

## CI/prek Hook

Add to `.pre-commit-config.yaml`:

```yaml
- id: check-agent-skills-in-sync
  name: Check agent skills are in sync with RST docs
  entry: python scripts/ci/prek/extract_agent_skills.py --check
  language: python
  pass_filenames: false
  files: ^contributing-docs/.*\.rst$|^\.github/skills/.*SKILL\.md$
  stages: [pre-commit, manual]
```

Fires when: any `contributing-docs/*.rst` changes (directive may have changed) OR any `.github/skills/*/SKILL.md` changes directly (drift detection).

---

## Verification Strategy

### Layer 1: Unit tests for extractor

`scripts/tests/ci/prek/test_extract_agent_skills.py`

- RST directive parsing → correct SkillData fields
- SKILL.md output matches known-good fixture for each skill
- Drift detection: modify RST → `--check` exits 1
- Drift detection: modify SKILL.md directly → `--check` exits 1
- Cycle detection, prereq validation (reuse patterns from `test_generate_agent_skills.py`)

### Layer 2: Test marker detection

`scripts/tests/ci/prek/test_context_detect.py` (additions)

Test fixtures in `scripts/tests/ci/prek/fixtures/`:

- `db_test_method.py` — `@pytest.mark.db_test` on a method
- `db_test_class.py` — `@pytest.mark.db_test` on a class
- `db_test_module.py` — `pytestmark = pytest.mark.db_test`
- `unit_test_plain.py` — no db_test markers

Assert:

- `has_db_test_marker(fixture)` returns correct bool for each
- `get_skill_for_test()` routes to `run-db-test` for db fixtures, `run-single-test` for plain
- `get_command()` for `run-db-test` contains `breeze`, never `uv run`

### Layer 3: Behavioral simulation

`scripts/tests/ci/prek/test_agent_behavior.py`

Given a test file with `@pytest.mark.db_test` → assert full command contains `breeze` and does not contain `uv run --project`.

This is the closest to behavioral testing without running an LLM. If `context_detect.py` routes correctly AND SKILL.md snapshot tests pass, an agent using both will behave correctly.

---

## Migration Phases

### Phase 1 — Embed RST directives in existing docs (no breaking change)

- Add `.. agent-skill::` blocks to 7 target RST files
- YAML files and old generator continue working unchanged
- Promotes the RST PoC pattern from `agent_skills.rst` to the real docs

### Phase 2 — Build extractor and new SKILL.md files

- Implement `extract_agent_skills.py`
- Add `has_db_test_marker()` + `get_skill_for_test()` to `context_detect.py`
- Add `run-db-test` skill (new, missing from PoC)
- Write tests (layers 1–3)
- Commit generated `.github/skills/*/SKILL.md` files

### Phase 3 — Register prek hook

- Add `check-agent-skills-in-sync` to `.pre-commit-config.yaml`
- CI enforces drift detection

### Phase 4 — Deprecate YAML pipeline (cleanup)

- Delete `contributing-docs/workflows/*.yaml`
- Delete `contributing-docs/workflows/agent_skills.rst` (comparison artifact)
- Remove or archive `generate_agent_skills.py` (replaced by extractor)
- Update `CLAUDE.md` to reference `.github/skills/` and the `run-db-test` routing rule

---

## File Summary

### New files

| File | Purpose |
|---|---|
| `scripts/ci/prek/extract_agent_skills.py` | RST parser + SKILL.md generator |
| `scripts/tests/ci/prek/test_extract_agent_skills.py` | Extractor tests |
| `scripts/tests/ci/prek/test_agent_behavior.py` | Behavioral routing tests |
| `scripts/tests/ci/prek/fixtures/db_test_*.py` | Test fixtures |
| `.github/skills/run-single-test/SKILL.md` | Generated (committed) |
| `.github/skills/run-db-test/SKILL.md` | New skill — Breeze-first rule |
| `.github/skills/setup-breeze-environment/SKILL.md` | Generated |
| `.github/skills/run-static-checks/SKILL.md` | Generated |
| `.github/skills/run-manual-checks/SKILL.md` | Generated |
| `.github/skills/build-docs/SKILL.md` | Generated |
| `.github/skills/run-provider-tests/SKILL.md` | New skill |

### Modified files

| File | Change |
|---|---|
| `contributing-docs/testing/unit_tests.rst` | Add `.. agent-skill::` for `run-single-test` and `run-db-test` |
| `contributing-docs/08_static_code_checks.rst` | Add directives for `run-static-checks`, `run-manual-checks` |
| `contributing-docs/11_documentation_building.rst` | Add directive for `build-docs` |
| `contributing-docs/06_development_environments.rst` | Add directive for `setup-breeze-environment` |
| `contributing-docs/12_provider_distributions.rst` | Add directive for `run-provider-tests` |
| `scripts/ci/prek/context_detect.py` | Add `has_db_test_marker()`, `get_skill_for_test()`, `get_command_for_test()` |
| `scripts/tests/ci/prek/test_context_detect.py` | Add tests for new functions |
| `.pre-commit-config.yaml` | Register `check-agent-skills-in-sync` hook |
| `CLAUDE.md` | Update to reference `.github/skills/` location |

### Deleted (Phase 4)

- `contributing-docs/workflows/*.yaml` (5 files)
- `contributing-docs/workflows/agent_skills.rst`
- `scripts/ci/prek/generate_agent_skills.py`
- `scripts/tests/ci/prek/test_generate_agent_skills.py`

---

## Answers to Mentor's Questions

**Q: What are Agent Skills and what's the problem with AGENTS.md/MCP?**
Agent Skills (agentskills.io SKILL.md) are modular, auto-discovered markdown files. AGENTS.md grows monolithic with no structure — the agent reads all of it every time, and commands like "run tests" have no decision logic encoded. MCP requires server infrastructure. SKILL.md = zero-infrastructure structured skills.

**Q: Does the approach solve core pain points?**
Yes: RST-first eliminates drift (one place to edit), `run-db-test` skill encodes the Breeze-first rule (agents stop guessing), `context_detect.py` + test marker detection provides programmatic routing, prek hook enforces consistency.

**Q: How to make contributor docs the source of truth?**
`.. agent-skill::` directives are embedded in existing RST files. The extractor reads ONLY from RST. YAML files are deleted. CI fails if SKILL.md is out of sync with RST.

**Q: Consistency between docs and agent skills?**
Same pattern as `update_breeze_cmd_output.py` — prek `--check` mode diffs re-generated vs committed, fails CI on any drift. Impossible to merge a docs change without the SKILL.md updating.

**Q: Long-term maintainability?**
Maintainers edit RST exactly as today. No YAML schema to learn. The directive syntax is simple (one `:option:` per line). Adding a new skill = adding a `.. agent-skill::` block in the right RST section. The prek hook handles the rest.
