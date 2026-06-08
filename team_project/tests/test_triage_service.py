"""Tests for triage summary orchestration."""

from __future__ import annotations

from unittest import mock

from dag_triage.classifier import FailureClassifier
from dag_triage.log_tail import LogTailResult
from dag_triage.remediation_kb import RemediationKB
from dag_triage.triage_service import build_triage_summary


def test_build_triage_summary_with_classification(mock_ti: mock.MagicMock) -> None:
    log_text = (
        "[2024-01-15T10:30:46.200+0000] {task.py:99} ERROR - Connection refused\n"
        "Traceback (most recent call last):\n"
        "ConnectionResetError: connection reset by peer"
    )
    log_result = LogTailResult(
        text=log_text,
        line_count=3,
        requested_lines=200,
        dag_id=mock_ti.dag_id,
        task_id=mock_ti.task_id,
        run_id=mock_ti.run_id,
        map_index=mock_ti.map_index,
        try_number=mock_ti.try_number,
        state="failed",
    )

    with mock.patch("dag_triage.triage_service.get_task_log_tail", return_value=log_result):
        summary = build_triage_summary(mock_ti)

    assert summary.log_available
    assert summary.categories
    assert summary.categories[0].name == "TRANSIENT"
    assert "Likely Transient failure" in summary.root_cause_summary
    assert summary.remediations
    assert summary.error is None


def test_build_triage_summary_without_log_content(mock_ti: mock.MagicMock) -> None:
    log_result = LogTailResult(
        text="",
        line_count=0,
        requested_lines=200,
        dag_id=mock_ti.dag_id,
        task_id=mock_ti.task_id,
        run_id=mock_ti.run_id,
        map_index=mock_ti.map_index,
        try_number=mock_ti.try_number,
        state="failed",
        error="Task log handler does not support read operations.",
    )

    with mock.patch("dag_triage.triage_service.get_task_log_tail", return_value=log_result):
        summary = build_triage_summary(mock_ti)

    assert not summary.log_available
    assert summary.categories == []
    assert "No automated classification" in summary.root_cause_summary
    assert summary.error == "Task log handler does not support read operations."


def test_build_triage_summary_uses_injected_dependencies(mock_ti: mock.MagicMock) -> None:
    classifier = FailureClassifier()
    kb = RemediationKB()
    log_result = LogTailResult(
        text="AttributeError: 'NoneType' object has no attribute 'run'",
        line_count=1,
        requested_lines=200,
        dag_id=mock_ti.dag_id,
        task_id=mock_ti.task_id,
        run_id=mock_ti.run_id,
        map_index=mock_ti.map_index,
        try_number=mock_ti.try_number,
        state="failed",
    )

    with mock.patch("dag_triage.triage_service.get_task_log_tail", return_value=log_result):
        summary = build_triage_summary(mock_ti, classifier=classifier, kb=kb)

    assert summary.categories[0].name == "CODE"
