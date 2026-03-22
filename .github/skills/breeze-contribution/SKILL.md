 <!-- SPDX-License-Identifier: Apache-2.0
      https://www.apache.org/licenses/LICENSE-2.0 -->

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

# Airflow Breeze Contribution Skill

**Version:** 0.1.0 (March 2026)
**Source of truth:** `contributing-docs/` and `dev/breeze/src/airflow_breeze/commands/developer_commands_config.py`

This skill encodes the standard Airflow contributor workflows so that AI agents
(Claude Code, Gemini CLI, GitHub Copilot, etc.) can execute them correctly
regardless of whether they are running on the **host** or **inside a Breeze container**.

---

## 1. Environment Detection (MUST CHECK FIRST)

Before running any command, the agent MUST detect its current context.

**You are inside Breeze if ANY of the following is true:**

- `/opt/airflow` directory exists (canonical Breeze container mount point)
- `/.dockerenv` file exists (Docker sets this in all containers)
- Environment variable `AIRFLOW_BREEZE_CONTAINER=true` is set

**You are on the HOST if none of the above are true.**

```python
import os
from pathlib import Path


def is_inside_breeze() -> bool:
    return (
        os.getenv("AIRFLOW_BREEZE_CONTAINER") == "true"
        or Path("/.dockerenv").exists()
        or Path("/opt/airflow").exists()
    )
```

---

## 2. Command Boundary Rules

NEVER run HOST-only commands inside Breeze. NEVER run CONTAINER-only commands on host directly.

| Workflow | HOST command | CONTAINER command |
| --- | --- | --- |
| Enter Breeze shell | `breeze shell` | N/A — already inside |
| Run targeted tests | `uv run --project {dist} pytest {test_path} -xvs` | `pytest {test_path} -xvs` |
| Run static checks | `prek` | `prek` |
| Start full Airflow | `breeze start-airflow` | N/A — already started |
| Build docs | `breeze build-docs` | N/A |
| Git operations | `git add / commit / push` | N/A — use host |

---

## 3. Core Contributor Workflows

### Workflow A: Static Checks

**Always runs on HOST.**

```bash
git add <changed_files>
prek
# Fix any failures reported, then repeat
```

If prek is not installed: `uv tool install prek`


---

### Workflow B: Run Targeted Tests

**Local-first. Breeze is fallback only.**

```bash
# Step 1: Try local first (faster, easier to debug)
uv run --project airflow-core pytest airflow-core/tests/unit/{test_path} -xvs

# Step 2: Only if Step 1 fails with missing system deps, fall back to Breeze
breeze exec -- pytest {test_path} -xvs
```

Signs you need Breeze fallback:

- `ModuleNotFoundError` for system-level packages (mysqlclient, libpq, etc.)
- Test results differ from CI


---

### Workflow C: System Behavior Verification (Stretch)

**HOST only.**

```bash
breeze start-airflow
breeze exec -- airflow dags trigger {dag_id}
breeze exec -- airflow tasks states-for-dag-run {dag_id} {run_id}
```


---

## 4. Agent Decision Tree

```
Receive task
|
+-> Detect context: is_inside_breeze()?
|
+-> NO (host):
|   +-> git add <files>
|   +-> prek  (fix failures, repeat)
|   +-> uv run --project {dist} pytest {path} -xvs
|       +-> Missing deps? -> breeze exec -- pytest {path} -xvs
|   +-> git push
|
+-> YES (inside Breeze):
    +-> pytest {path} -xvs
    +-> Do NOT run breeze shell / breeze start-airflow
```

---

## 5. Key Files

| File | Purpose |
| --- | --- |
| `dev/breeze/src/airflow_breeze/commands/developer_commands_config.py` | Canonical Breeze command list |
| `.pre-commit-config.yaml` | All prek hooks — see `update-breeze-cmd-output` for sync pattern |
| `contributing-docs/` | Human workflow docs (source of truth) |
| `AGENTS.md` | Top-level agent instructions |

---

## 6. DO / DON'T

**DO:**

- Always detect context before choosing a command
- Run tests local-first; use Breeze only as fallback for missing system deps
- Use `breeze exec --` to run single commands in an existing container from host
- Keep all git operations on the HOST

**DON'T:**

- Run `breeze shell` or `breeze start-airflow` inside an existing Breeze container
- Run the full test suite — always target specific paths with `-xvs`
- Ignore prek failures — they will block CI
