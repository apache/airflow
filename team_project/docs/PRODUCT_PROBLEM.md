# Problem Framing: AI-Assisted Dag Failure Triage

Canonical product problem document for Milestone Mavericks. This framing is the
prerequisite for the PRFAQ press release. Indexed from
[`prfaq/README.md`](prfaq/README.md).

## Problem Statement

Platform engineers running production Apache Airflow spend significant time
manually parsing task logs to diagnose Dag failures. The native Task Instance
view surfaces raw logs only, with no failure categorization, root-cause
summary, or remediation guidance. This creates unnecessary latency in incident
response and places a high cognitive burden on on-call engineers.

Machine-learning engineers and operational data users hit the same wall: they
open the Task Instance view under alert pressure, scroll unstructured logs, and
cannot quickly tell whether to retry, escalate, or fix — especially when
failures span upstream data boundaries or platform infrastructure.

## Outcome Metric

Reduce median time-to-first-diagnosis on failed Dag tasks from unassisted
manual log inspection to under two minutes, measured on a curated Airflow
failure corpus from Dag run failed state to engineer-identified root-cause
category.

## Target Personas

Three primary users share this problem with different emphasis:

| Persona | Primary pain |
|---------|--------------|
| Platform Engineer | Manual log triage at scale; no built-in failure categorization |
| ML Engineer | Cannot attribute failures (data vs. code vs. resources) without Airflow depth |
| On-Call Data Engineer | Raw logs are not actionable; no guided triage under SLA pressure |

Full persona definitions, daily workflows, pain points, and upstream Apache
Airflow issue evidence are in [`PERSONAS.md`](PERSONAS.md).

## Solution Boundary

Extends the Task Instance view (read-only plugin surface) to display:

- Failure category: one of TRANSIENT, DATA_QUALITY, RESOURCE, CODE,
  EXTERNAL_DEPENDENCY
- Root-cause summary derived from normalized task logs
- Remediation checklist of at least three suggested next steps with
  links to official Airflow documentation

The classifier returns ranked candidates with confidence scores rather than a
single forced label, so engineers retain judgment in ambiguous cases.

The feature is opt-in via an Airflow configuration flag and disabled by
default. No modifications to the Airflow core scheduler or executor are
required. A local-only mode bypasses external LLM calls for data-residency
constraints.

Technical architecture and layer boundaries are documented in
[`ARCHITECTURE.md`](ARCHITECTURE.md). Sprint delivery plan is in
[`PROJECT_PLAN.md`](PROJECT_PLAN.md).

## Team Sign-Off

| Reviewer | Role | Approval |
|----------|------|----------|
| Sharan Saravanan | Project scoping and engineering (author) | Pending |
| Poorani T S | Engineering and coordination | Pending |
| Sohail Anwar | Scrum Master | Pending |

Formal sign-off is recorded by updating this table and via pull-request
approval on the commit linked to issue #17. All three approvals are required
before this document is considered finalized.
