# Milestone Mavericks — Team Workspace

> **DAG Triage Assistant Plugin** — an AI-assisted failure triage plugin for Apache Airflow that automatically classifies task-instance logs into failure categories (transient, resource, code, data quality, external dependency), scores confidence, and returns actionable remediation steps — all without an external LLM call.
> See [`plugin/README.md`](plugin/README.md) for installation, API reference, and the standalone demo.

## Team

| Name | Role |
|------|------|
| Sohail Anwar | Scrum Master |
| Poorani T S | Team Member |
| Sharan Saravanan | Team Member |

## Course

**CSS 566A — Software Management**
University of Washington Bothell · Spring 2026
Instructor: Prof. Mia Champion

## Project Scope

We are building an **AI-assisted DAG failure triage plugin** for Apache Airflow.
The plugin ingests task-instance logs from a failing DAG run, classifies the
failure using a lightweight heuristic layer followed by an LLM summarization
layer, and surfaces ranked remediation candidates to the on-call engineer.
A local-only execution mode ensures the plugin can run without sending log data
to an external API, satisfying data-residency requirements.

## Documentation

| Document | Description |
|----------|-------------|
| [`docs/prfaq/README.md`](docs/prfaq/README.md) | PRFAQ working folder — index of problem framing and related docs |
| [`docs/PRODUCT_PROBLEM.md`](docs/PRODUCT_PROBLEM.md) | Canonical problem framing (issue #17) |
| [`docs/PERSONAS.md`](docs/PERSONAS.md) | Target personas and upstream issue evidence |
| [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) | Plugin architecture |
| [`docs/PROJECT_PLAN.md`](docs/PROJECT_PLAN.md) | Sprint plan |

## Links

| Resource | URL |
|----------|-----|
| Fork | https://github.com/break-through-19/airflow |
| Kanban board | https://github.com/users/break-through-19/projects/9 |
