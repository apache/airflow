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

# Breeze-Aware Agent Skills from Executable Contributor Workflows

## Problem

Airflow contributor workflows are not generic Python workflows. Agents that run plain `pytest` can fail because Airflow uses specific execution patterns (`uv run --project ...`, `breeze run ...`).

This PoC keeps documentation as source of truth and generates open-standard Agent Skills folders that any AI agent can consume.

## Open Standard Compliance

This implementation follows the Agent Skills directory standard:

- each skill is a directory under `skills/`
- each skill contains `SKILL.md`
- `SKILL.md` includes YAML frontmatter with `name` and `description`
- no custom `skills.json` schema is used

## Architecture

`docs/CONTRIBUTING_POC.rst`
-> parse `.. agent-skill::` blocks with `docutils`
-> validate `Workflow` objects
-> generate `skills/<workflow-id>/SKILL.md`

Environment-aware behavior is described inside each `SKILL.md` instructions section (Local vs Breeze execution paths).

## Project Structure

- `docs/CONTRIBUTING_POC.rst`: executable contributor workflows
- `model.py`: `Workflow` model + validation
- `parser.py`: RST parser using `docutils`
- `generator.py`: workflow -> standard `SKILL.md` generation
- `generate_agent_skills.py`: CLI entrypoint
- `benchmark.py`: real parse/generation benchmarks
- `skills/`: generated standard Agent Skills directories
- `tests/`: parser/model/generator/sync tests

## Generate Skills

From `agent_skills_poc/`:

```bash
uv run --project ../scripts --with docutils python generate_agent_skills.py docs/CONTRIBUTING_POC.rst
```

This generates skill folders under `skills/`.

## Run Tests

From `agent_skills_poc/`:

```bash
uv run --project ../scripts --with docutils pytest tests -xvs
```

Key validations include:

- valid/invalid workflow parsing
- model validation errors
- real file generation of `skills/<id>/SKILL.md`
- YAML frontmatter presence and required keys
- docs/skills sync guard

## Run Benchmarks

From `agent_skills_poc/`:

```bash
uv run --project ../scripts --with docutils python benchmark.py
```

Benchmark is real (no fabricated values) and measures:

- parsing time
- skill generation time
- memory usage

for 1, 10, and 100 workflows using `time.perf_counter` and `tracemalloc`.
