"""Tests for the heuristic failure classifier."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest

from dag_triage.classifier import FailureClassifier


@pytest.fixture
def clf() -> FailureClassifier:
    return FailureClassifier()


def top_category(results: list[tuple[str, float]]) -> str:
    assert results, "Expected at least one category but got empty list"
    return results[0][0]


# ---------------------------------------------------------------------------
# TRANSIENT
# ---------------------------------------------------------------------------


def test_transient_timeout(clf: FailureClassifier) -> None:
    log = "Task failed after 30s: connection reset by peer, timeout exceeded"
    assert top_category(clf.classify(log)) == "TRANSIENT"


def test_transient_503_retry_exhausted(clf: FailureClassifier) -> None:
    log = "Received HTTP 503 Service Unavailable. Retry exhausted after 5 attempts."
    assert top_category(clf.classify(log)) == "TRANSIENT"


# ---------------------------------------------------------------------------
# DATA_QUALITY
# ---------------------------------------------------------------------------


def test_data_quality_null_value(clf: FailureClassifier) -> None:
    log = "Insert failed: null value in column 'user_id' violates not-null constraint"
    assert top_category(clf.classify(log)) == "DATA_QUALITY"


def test_data_quality_schema_mismatch(clf: FailureClassifier) -> None:
    log = "Schema mismatch detected: expected INT got STRING. Validation error on row 42."
    assert top_category(clf.classify(log)) == "DATA_QUALITY"


# ---------------------------------------------------------------------------
# RESOURCE
# ---------------------------------------------------------------------------


def test_resource_oom(clf: FailureClassifier) -> None:
    log = "java.lang.OutOfMemoryError: Java heap space"
    assert top_category(clf.classify(log)) == "RESOURCE"


def test_resource_oomkilled(clf: FailureClassifier) -> None:
    log = "Container OOMKilled: memory limit of 512Mi exceeded"
    assert top_category(clf.classify(log)) == "RESOURCE"


# ---------------------------------------------------------------------------
# CODE
# ---------------------------------------------------------------------------


def test_code_type_error(clf: FailureClassifier) -> None:
    log = "TypeError: unsupported operand type(s) for +: 'int' and 'str'"
    assert top_category(clf.classify(log)) == "CODE"


def test_code_import_error(clf: FailureClassifier) -> None:
    log = "ImportError: No module named 'pandas'. Check your dependencies."
    assert top_category(clf.classify(log)) == "CODE"


# ---------------------------------------------------------------------------
# EXTERNAL_DEPENDENCY
# ---------------------------------------------------------------------------


def test_external_dependency_ssl(clf: FailureClassifier) -> None:
    log = "SSL: CERTIFICATE_VERIFY_FAILED certificate verify failed (_ssl.c:1129)"
    assert top_category(clf.classify(log)) == "EXTERNAL_DEPENDENCY"


def test_external_dependency_dns(clf: FailureClassifier) -> None:
    log = "DNS resolution failed: host unreachable for endpoint db.internal"
    assert top_category(clf.classify(log)) == "EXTERNAL_DEPENDENCY"


# ---------------------------------------------------------------------------
# Unclassifiable — should return empty list
# ---------------------------------------------------------------------------


def test_unclassifiable_returns_empty(clf: FailureClassifier) -> None:
    log = "Task started successfully. All checks passed."
    assert clf.classify(log) == []


# ---------------------------------------------------------------------------
# General contract: result is sorted descending by confidence
# ---------------------------------------------------------------------------


def test_results_sorted_descending(clf: FailureClassifier) -> None:
    log = "TypeError: bad type. Also got 503 timeout."
    results = clf.classify(log)
    confidences = [conf for _, conf in results]
    assert confidences == sorted(confidences, reverse=True)
