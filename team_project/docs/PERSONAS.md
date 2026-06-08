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

**Upstream evidence:**

| Issue | Pain point | Summary |
|-------|------------|---------|
| [apache/airflow#28116](https://github.com/apache/airflow/issues/28116) | No built-in failure categorization; repeated transient failures indistinguishable from real failures without log context | Scheduler-detected failures (for example zombie jobs) are marked failed in the UI with no reason in the Task Instance Logs tab, so platform operators must hunt through scheduler logs to learn whether the root cause is infrastructure or task logic. |

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

**Upstream evidence:**

| Issue | Pain point | Summary |
|-------|------------|---------|
| [apache/airflow#43171](https://github.com/apache/airflow/issues/43171) | Cannot tell data quality vs. resource constraint without deep log inspection; escalation to platform team is the default | The Airflow Debugging Survey 2024 found 41.7% of respondents do not consider native error messages actionable, leaving DAG consumers without Airflow expertise unable to attribute failures on their own. |

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

**Upstream evidence:**

| Issue | Pain point | Summary |
|-------|------------|---------|
| [apache/airflow#63736](https://github.com/apache/airflow/issues/63736) | Raw traceback text is not actionable; time-to-first-diagnosis extends beyond incident SLA | Exception stack traces stored in Elasticsearch were silently dropped from the Task Instance logs tab, leaving on-call engineers with only "Task failed with exception" and no traceback to act on. |