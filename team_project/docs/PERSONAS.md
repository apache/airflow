# Target User Personas

## Persona 1: Platform Engineer

**Role:** Platform Engineer, production Airflow operator
**Context:** Manages 50+ DAGs across multiple teams on a self-hosted or
cloud-managed Airflow deployment.

**Daily workflow:** Monitors DAG run status, responds to failure alerts,
manually opens Task Instance logs to identify root cause, coordinates with
data engineers to apply fixes.

**Pain points:**
- Parsing multi-hundred-line task logs manually to find the one relevant error
- No built-in failure categorization means every incident starts from scratch
- Repeated transient failures are indistinguishable from real failures without
  log context

**Upstream evidence:** apache/airflow issues referencing manual log triage
overhead in production operator workflows.

---

## Persona 2: ML Engineer

**Role:** ML Engineer, DAG consumer
**Context:** Authors and maintains ML pipeline DAGs but does not manage
the Airflow infrastructure.

**Daily workflow:** Submits DAG runs for model training or data preprocessing,
monitors for failures, struggles to distinguish upstream data failures from
own code failures.

**Pain points:**
- Unfamiliar with Airflow internals, making raw log interpretation slow
- Cannot tell whether a failure is a data quality issue or a resource constraint
  without deep log inspection
- No structured root-cause output means escalation to platform team is the
  default, adding latency

**Upstream evidence:** apache/airflow community posts referencing confusion
over failure attribution in multi-team DAG environments.

---

## Persona 3: Operational Data User / On-Call Engineer

**Role:** On-call data engineer or analyst with operational responsibilities
**Context:** Rotates into on-call coverage for data pipelines without deep
Airflow expertise.

**Daily workflow:** Receives PagerDuty or Slack alert for failed DAG, opens
Airflow UI, reads raw task log, attempts to determine whether to retry,
escalate, or fix.

**Pain points:**
- Raw traceback text is not actionable without framework knowledge
- No remediation guidance means every failure requires senior engineer
  consultation
- Time-to-first-diagnosis extends beyond acceptable incident SLA without
  structured triage

**Upstream evidence:** apache/airflow operator community discussions on
improving Task Instance failure visibility for non-expert operators.