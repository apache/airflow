# Agent Skills PoC

## Problem

Apache Airflow contributors follow repository-specific workflows for tests, static checks, and
environment debugging. Generic AI coding agents often run incorrect commands because they do not
learn from contributor documentation.

**Traditional approach:** Agents try to execute generic commands like `pytest` directly, which fails in Airflow.

**PoC solution:** Executable documentation captures workflows as structured data that agents can safely consume.

## Executable Documentation Concept

Instead of prose-only documentation, embed structured workflow metadata directly in RST files:

```rst
.. agent-skill::
   :id: run_tests
   :description: Run unit tests locally then fallback to Breeze
   :local: uv run --project distribution_folder pytest
   :fallback: breeze exec pytest
```

The parser extracts these blocks and generates JSON that agents can programmatically consume.

### Realistic Airflow Example

This PoC now includes a realistic command from Airflow contributor docs:

```rst
.. agent-skill::
    :id: run_single_core_test
    :description: Run one airflow-core pytest target locally, fallback to Breeze when needed
    :local: uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs
    :fallback: breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs
```

## Full Architecture Pipeline

```
CONTRIBUTING_POC.rst
    ↓ (docutils parser)
structured workflow blocks
    ↓ (workflow model)
internal Workflow objects
    ↓ (JSON generator)
generated skills.json
    ↓ (runtime)
context detection (host vs Breeze)
    ↓
command resolution
    ↓
agents receive correct command
```

### Pipeline Stages

1. **Documentation Parsing**: docutils extracts `.. agent-skill::` directives from RST.
2. **Validation**: Workflow model validates id, description, and commands.
3. **JSON Generation**: Serialize workflows into agents-readable JSON.
4. **Runtime Context Detection**: Detect execution environment (host machine vs Breeze container).
5. **Command Resolution**: Load skills.json and resolve correct command for current environment.

### Why This Matters

- **Single source of truth**: Documentation is the authoritative workflow definition.
- **Environmental awareness**: Different commands for host vs container without duplication.
- **Agent-safe consumption**: Agents receive validated, structured JSON—not raw documentation.
- **Extensible**: New workflow types don't require code changes, only documentation updates.

## Layout

```
agent_skills_poc/
├── docs/
│   └── CONTRIBUTING_POC.rst          # Source-of-truth workflow definitions
├── model/
│   └── workflow.py                   # Workflow dataclass with validation
├── parser/
│   └── parser.py                     # docutils-based RST parser
├── generator/
│   └── generate_skills.py            # Skills JSON generator + CLI
├── context.py                        # Runtime environment detection
├── resolver.py                       # Command resolution from skills
├── benchmarks/
│   └── benchmark_parser.py           # Performance benchmarks (real measurements)
│   └── benchmark_resolver.py         # Dedicated resolver-latency benchmark
├── tests/
│   ├── test_parser.py
│   ├── test_generator.py
│   ├── test_context.py
│   └── test_resolver.py
│   └── test_sync.py                  # CI guard for docs vs skills.json drift
├── output/
│   └── skills.json                   # Generated output artifact
└── README.md                         # This file
```

## How Agents Consume Skills

1. Call `resolve_command(skill_id)` with optional environment context.
2. Function loads `skills.json` and detects runtime environment.
3. Returns the correct command string for execution.
4. Agent executes the command without worrying about environment differences.

Example usage:

```python
from agent_skills_poc import resolve_command

# Automatically detects environment and returns appropriate command
cmd = resolve_command("run_tests")
# On host: "uv run --project distribution_folder pytest"
# In Breeze: "uv run --project distribution_folder pytest"
# Falls back to: "breeze exec pytest" if needed
```

More explicit agent-side usage:

```python
from pathlib import Path

from agent_skills_poc import detect_context, resolve_command

context = detect_context()
skills_path = Path("agent_skills_poc/output/skills.json")

# The agent consumes generated JSON only.
command = resolve_command("run_single_core_test", skills_path=skills_path, context=context)
print(command)
```

## Environment Detection

Context detection happens automatically with priority:

1. Check `AIRFLOW_BREEZE_CONTAINER` environment variable.
2. Check for `/.dockerenv` file (Docker/container marker).
3. Check for `/opt/airflow` directory (Airflow-specific path).
4. Default to `host` if none found.

## Workflow Validation

The parser validates all extracted workflows:

- **id**: Must match `/^[A-Za-z0-9_-]+$/` (no spaces or special chars).
- **description**: Non-empty string describing the workflow.
- **local**: Non-empty command for host execution.
- **fallback**: Non-empty command for fallback scenarios.

Invalid workflows raise clear `WorkflowValidationError` exceptions.

## Commands

### Generate Skills

From repository root:

```bash
uv run --project scripts python agent_skills_poc/generator/generate_skills.py \
  agent_skills_poc/docs/CONTRIBUTING_POC.rst \
  agent_skills_poc/output/skills.json
```

Or from within `agent_skills_poc/`:

```bash
uv run --project scripts python generate_skills.py docs/CONTRIBUTING_POC.rst output/skills.json
```

### Test Everything

```bash
uv run --project scripts --with docutils pytest agent_skills_poc/tests -xvs
```

This runs:

- Parser correctness tests
- Workflow validation tests
- Context detection tests
- Command resolution tests
- JSON schema validation

### Run Real Benchmarks

```bash
uv run --project scripts --with docutils python agent_skills_poc/benchmarks/benchmark_parser.py
```

Run dedicated resolver-latency benchmark:

```bash
uv run --project scripts --with docutils python agent_skills_poc/benchmarks/benchmark_resolver.py
```

Output example:

```
Workflows: 1
Parsing time: 6.184 ms
Skill generation time: 0.004 ms
Context detection time: 0.012 ms
Command resolution time: 0.025 ms
Memory usage: 1.221 MB
-
Workflows: 10
Parsing time: 16.346 ms
Skill generation time: 0.023 ms
Context detection time: 0.011 ms
Command resolution time: 0.030 ms
Memory usage: 2.046 MB
-
Workflows: 100
Parsing time: 148.346 ms
Skill generation time: 0.260 ms
Context detection time: 0.012 ms
Command resolution time: 0.050 ms
Memory usage: 2.384 MB
```

All numbers are real measurements from `time.perf_counter` and `tracemalloc`.

### CI Divergence Guard (Docs vs Generated Skills)

To ensure docs remain the source of truth, CI should run:

```bash
uv run --project scripts --with docutils pytest agent_skills_poc/tests/test_sync.py -xvs
```

`test_sync.py` regenerates skills from `docs/CONTRIBUTING_POC.rst` and compares against committed
`output/skills.json`. If documentation changes without regenerating skills, this test fails.

## Integration with AI Agents

Agents that consume Airflow skills should:

1. Load skills at startup: `load_skills(Path("agent_skills_poc/output/skills.json"))`
2. When executing a workflow, call: `resolve_command(skill_id)`
3. Execute the returned command string.
4. Log skill usage for debugging.

Example:

```python
from agent_skills_poc import resolve_command, ExecutionContext, detect_context

# Detect environment
context = detect_context()
print(f"Running in: {context.environment}")

# Resolve and execute a skill
try:
    cmd = resolve_command("run_tests", context=context)
    print(f"Executing: {cmd}")
    subprocess.run(cmd, shell=True, check=True)
except CommandResolutionError as err:
    print(f"Failed to resolve command: {err}")
```

## Future Improvements

- Multi-environment support (Windows WSL, Mac, native Docker).
- Skill dependencies (skill A must run before skill B).
- Conditional execution based on changed files.
- Skill metrics and success/failure tracking.
- Integration with Airflow's execution API.
