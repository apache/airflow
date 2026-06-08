# Product Problem: AI-Assisted DAG Failure Triage

## Problem Statement

Platform engineers running production Apache Airflow spend significant time
manually parsing task logs to diagnose DAG failures. The native Task Instance
view surfaces raw logs only, with no failure categorization, root-cause
summary, or remediation guidance. This creates unnecessary latency in incident
response and places a high cognitive burden on on-call engineers.

## Outcome Metric

Reduce median time-to-first-diagnosis on failed DAG tasks from unassisted
manual log inspection to under two minutes, measured on a curated Airflow
failure corpus from DAG run failed state to engineer-identified root-cause
category.

## Solution Boundary

Extends the Task Instance view (read-only plugin surface) to display:

- Failure category: one of TRANSIENT, DATA_QUALITY, RESOURCE, CODE,
  EXTERNAL_DEPENDENCY
- Root-cause summary derived from normalized task logs
- Remediation checklist of at least three suggested next steps with
  links to official Airflow documentation

The feature is opt-in via an Airflow configuration flag and disabled by
default. No modifications to the Airflow core scheduler or executor are
required.

## Architecture

The plugin implements a four-layer pipeline:

  Raw Airflow logs
        |
        v
  Log Normalization Layer   -> emits structured LogRecord dataclass
        |
        v
  Heuristic Classifier      -> returns ranked (category, confidence) list
        |
        v
  LLM Summarization Layer   -> optional; skipped when local_only=true
        |
        v
  Remediation KB + UI       -> displays runbooks alongside ranked candidates

Ranked candidates are returned rather than a single label to preserve
engineer judgment in ambiguous multi-signal failure cases.

## Implementation Evidence

| Layer | Module | Tests |
|-------|--------|-------|
| Log normalization | src/dag_triage/log_parser.py | 4 passing |
| Heuristic classification | src/dag_triage/classifier.py | 12 passing |
| Remediation lookup | src/dag_triage/remediation_kb.py | 10 passing |

Total: 26 tests passing across three modules.

## Acceptance Criteria Status

| Criterion | Status |
|-----------|--------|
| Triage section visible on failed Task Instance | Sprint 5 - UI |
| Failure category displayed | Done - classifier.py |
| Root-cause summary from task logs | Done - log_parser.py |
| Remediation checklist (3+ steps) | Done - remediation_kb.py |
| Opt-in config flag, disabled by default | Sprint 5 - plugin registration |