# Release Notes

## v1.0.0 (2026-06-09)

First stable release of the DAG Triage Assistant plugin for Apache Airflow 3.x.

### Features

- **Heuristic failure classifier** — regex-based classifier that categorizes task
  failures into five categories: TRANSIENT, DATA_QUALITY, RESOURCE, CODE, and
  EXTERNAL_DEPENDENCY, with ranked confidence scores. (#43)

- **Log normalization layer** — parses raw Airflow task-instance logs into
  structured `LogRecord` objects with timestamp, level, source, message, and
  traceback extraction. (#44)

- **Remediation knowledge base** — YAML-backed KB with eight Airflow failure
  patterns providing actionable remediation steps and documentation links. (#45)

- **Log tail capture** — extracts and caches the last N log lines from failed
  task instances via the Airflow listener interface. (#31)

- **Triage summary panel** — FastAPI sub-app mounted at `/triage-panel/` that
  renders an interactive triage view on the Task Instance page with failure
  category, confidence score, root-cause summary, and remediation steps. (#29, #33)

- **Remediation checklist generator** — produces step-by-step remediation
  checklists from failure categories for operator handoff. (#34)

- **LLM provider abstraction** — pluggable `LLMProvider` interface with two
  implementations: `OpenAIProvider` (hosted LLM) and `LocalRuleProvider`
  (deterministic rules, zero external calls). Provider selection driven by
  `[triage] llm_provider` in `airflow.cfg`. Automatic fallback to local rules
  on provider failure. (#32)

- **Docker Compose dev environment** — one-command `docker compose up` brings
  up Airflow with the plugin pre-installed, a sample failing DAG for manual
  triage testing, and source-mounted volumes for live reloading. (#28)

- **Classifier test suite** — TDD-driven unit tests covering all five failure
  categories, confidence ordering, and edge cases. (#35)

### Documentation

- Product problem framing document and PRFAQ index. (#1, #17)
- Target user personas with upstream Airflow issue evidence. (#2, #19)
- Architecture decision record for LLM provider selection. (#36)
- Plugin architecture overview and sprint plan.

### Installation

```bash
# From the fork:
pip install "git+https://github.com/break-through-19/airflow.git#subdirectory=team_project"

# With OpenAI support:
pip install "dag-triage[openai] @ git+https://github.com/break-through-19/airflow.git#subdirectory=team_project"

# Local editable install:
cd team_project && pip install -e .
```

### Configuration

```ini
[triage]
enabled = true
log_tail_lines = 200
llm_provider = local          # or "openai"
# openai_api_key = sk-...     # required when llm_provider = openai
```

### Requirements

- Apache Airflow >= 3.0
- Python >= 3.10
- PyYAML >= 6.0
- FastAPI >= 0.100
