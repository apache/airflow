"""demo_triage.py
Milestone Mavericks, CSS 566A, UW Bothell, Spring 2026.

Runs the full triage pipeline on the sample failed DAG log output
to demonstrate end-to-end classification and remediation lookup.
"""

from dag_triage.log_parser import parse_log
from dag_triage.classifier import FailureClassifier
from dag_triage.checklist_generator import generate_checklist

SAMPLE_LOG = (
    "[2026-06-01T10:00:00.000+0000] {sample_failed_dag.py:18} ERROR - "
    "host unreachable: DNS resolution failed for external-db.example.com. "
    "Connection 'external_db' is not configured in Airflow connections."
)


def run_demo():
    records = parse_log(SAMPLE_LOG)
    full_text = " ".join(
        r.message + (r.traceback or "") for r in records
    )

    candidates = FailureClassifier().classify(full_text)
    print("Classified failure candidates:")
    for category, confidence in candidates:
        print(f"  {category}: {confidence:.2f}")

    if candidates:
        top_category = candidates[0][0]
        checklist = generate_checklist(top_category, full_text)
        print(f"\nRemediation checklist for {top_category}:")
        for i, item in enumerate(checklist.items, 1):
            print(f"  {i}. {item.step}")


if __name__ == "__main__":
    run_demo()
