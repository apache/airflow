"""Orchestrate log tail, classification, and remediation lookup into a triage summary."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dag_triage.classifier import FailureClassifier
from dag_triage.log_tail_store import get_task_log_tail
from dag_triage.remediation_kb import Remediation, RemediationKB

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory

_ERROR_LINE = re.compile(
    r"^\[.*?]\s+\{[^}]+\}\s+ERROR\s+-\s+(.*)$",
    re.MULTILINE,
)


@dataclass(frozen=True)
class TriageCategory:
    name: str
    confidence: float


@dataclass(frozen=True)
class TriageRemediation:
    title: str
    steps: list[str]
    doc_links: list[str]


@dataclass(frozen=True)
class TriageSummary:
    categories: list[TriageCategory]
    root_cause_summary: str
    remediations: list[TriageRemediation]
    log_available: bool
    error: str | None = None


def _extract_last_error_line(log_text: str) -> str | None:
    matches = _ERROR_LINE.findall(log_text)
    if matches:
        return matches[-1].strip()
    for line in reversed(log_text.splitlines()):
        stripped = line.strip()
        if stripped and not stripped.startswith("::"):
            return stripped
    return None


def _build_root_cause_summary(
    categories: list[TriageCategory],
    log_text: str,
) -> str:
    last_error = _extract_last_error_line(log_text)
    if not categories:
        if last_error:
            return f"No automated category matched. Last error line: {last_error}"
        return "No automated classification available. Review the task logs below for error details."

    top = categories[0]
    confidence_pct = int(top.confidence * 100)
    if last_error:
        return (
            f"Likely {top.name.replace('_', ' ').title()} failure "
            f"({confidence_pct}% confidence). Last error: {last_error}"
        )
    return (
        f"Likely {top.name.replace('_', ' ').title()} failure "
        f"({confidence_pct}% confidence). Review the log tail for details."
    )


def _to_remediation_items(entries: list[Remediation]) -> list[TriageRemediation]:
    return [
        TriageRemediation(title=item.title, steps=list(item.steps), doc_links=list(item.doc_links))
        for item in entries
    ]


def build_triage_summary(
    task_instance: TaskInstance | TaskInstanceHistory,
    *,
    classifier: FailureClassifier | None = None,
    kb: RemediationKB | None = None,
) -> TriageSummary:
    """Build a triage summary for a task instance from cached or fresh log tail."""
    classifier = classifier or FailureClassifier()
    kb = kb or RemediationKB()

    log_result = get_task_log_tail(task_instance)
    log_text = log_result.text
    log_available = bool(log_text.strip())

    ranked = classifier.classify(log_text)
    categories = [TriageCategory(name=name, confidence=score) for name, score in ranked]

    remediations: list[TriageRemediation] = []
    if ranked:
        top_category = ranked[0][0]
        signature = log_text[:2000] if log_text else ""
        remediations = _to_remediation_items(kb.lookup(top_category, signature))

    summary_error = log_result.error
    if not log_available and summary_error is None:
        summary_error = "No log content available for this task instance."

    return TriageSummary(
        categories=categories,
        root_cause_summary=_build_root_cause_summary(categories, log_text),
        remediations=remediations,
        log_available=log_available,
        error=summary_error,
    )
