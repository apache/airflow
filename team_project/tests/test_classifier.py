"""Tests for the heuristic failure classifier.

Each regex rule in ``dag_triage.classifier._RULES`` has a dedicated parametrized
case. When adding a new classification rule, follow red-green-refactor:

1. Add a failing parametrized case here (red).
2. Implement the pattern in ``FailureClassifier`` (green).
3. Refactor shared weights or naming without changing behaviour (refactor).
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest

from dag_triage.classifier import FailureClassifier

RULE_CASES = [
    pytest.param("timeout", "TRANSIENT", "Request timeout after 30s", id="transient-timeout"),
    pytest.param(
        "connection_reset",
        "TRANSIENT",
        "Task failed: connection reset by peer",
        id="transient-connection-reset",
    ),
    pytest.param("503", "TRANSIENT", "Received HTTP 503 Service Unavailable", id="transient-503"),
    pytest.param("504", "TRANSIENT", "Upstream returned HTTP 504 Gateway Timeout", id="transient-504"),
    pytest.param(
        "retry_exhausted",
        "TRANSIENT",
        "Retry exhausted after 5 attempts",
        id="transient-retry-exhausted",
    ),
    pytest.param(
        "null_value",
        "DATA_QUALITY",
        "Insert failed: null value in column 'user_id'",
        id="data-quality-null-value",
    ),
    pytest.param(
        "schema_mismatch",
        "DATA_QUALITY",
        "Schema mismatch detected: expected INT got STRING",
        id="data-quality-schema-mismatch",
    ),
    pytest.param(
        "validation_error",
        "DATA_QUALITY",
        "Validation error on row 42",
        id="data-quality-validation-error",
    ),
    pytest.param(
        "type_mismatch",
        "DATA_QUALITY",
        "Type mismatch: expected int, got str",
        id="data-quality-type-mismatch",
    ),
    pytest.param(
        "out_of_memory",
        "RESOURCE",
        "java.lang.OutOfMemoryError: Java heap space",
        id="resource-out-of-memory",
    ),
    pytest.param(
        "oomkilled",
        "RESOURCE",
        "Container OOMKilled: memory limit of 512Mi exceeded",
        id="resource-oomkilled",
    ),
    pytest.param("disk_full", "RESOURCE", "Cannot write: disk full on /data", id="resource-disk-full"),
    pytest.param(
        "memory_limit",
        "RESOURCE",
        "Pod exceeded memory limit of 1Gi",
        id="resource-memory-limit",
    ),
    pytest.param(
        "type_error",
        "CODE",
        "TypeError: unsupported operand type(s) for +: 'int' and 'str'",
        id="code-type-error",
    ),
    pytest.param("key_error", "CODE", "KeyError: 'missing_key'", id="code-key-error"),
    pytest.param(
        "attribute_error",
        "CODE",
        "AttributeError: 'NoneType' object has no attribute 'value'",
        id="code-attribute-error",
    ),
    pytest.param("name_error", "CODE", "NameError: name 'undefined_var' is not defined", id="code-name-error"),
    pytest.param(
        "import_error",
        "CODE",
        "ImportError: No module named 'pandas'",
        id="code-import-error",
    ),
    pytest.param(
        "dns",
        "EXTERNAL_DEPENDENCY",
        "DNS resolution failed for db.internal",
        id="external-dns",
    ),
    pytest.param(
        "host_unreachable",
        "EXTERNAL_DEPENDENCY",
        "Host unreachable for endpoint db.internal",
        id="external-host-unreachable",
    ),
    pytest.param(
        "certificate",
        "EXTERNAL_DEPENDENCY",
        "Certificate verify failed: invalid certificate chain",
        id="external-certificate",
    ),
    pytest.param(
        "ssl",
        "EXTERNAL_DEPENDENCY",
        "SSL: CERTIFICATE_VERIFY_FAILED certificate verify failed",
        id="external-ssl",
    ),
    pytest.param(
        "refused",
        "EXTERNAL_DEPENDENCY",
        "Connection refused on port 443",
        id="external-refused",
    ),
]


@pytest.fixture
def clf() -> FailureClassifier:
    return FailureClassifier()


@pytest.mark.parametrize(("rule_id", "expected_category", "log"), RULE_CASES)
def test_classification_rule(
    clf: FailureClassifier,
    rule_id: str,
    expected_category: str,
    log: str,
) -> None:
    """Each rule fires independently and ranks its category first."""
    results = clf.classify(log)
    assert results, f"Rule {rule_id!r} should produce at least one category"
    top_category, confidence = results[0]
    assert top_category == expected_category
    assert confidence > 0


def test_unclassifiable_returns_empty(clf: FailureClassifier) -> None:
    log = "Task started successfully. All checks passed."
    assert clf.classify(log) == []


def test_empty_log_returns_empty(clf: FailureClassifier) -> None:
    assert clf.classify("") == []


def test_results_sorted_descending(clf: FailureClassifier) -> None:
    log = "TypeError: bad type. Also got 503 timeout."
    results = clf.classify(log)
    confidences = [conf for _, conf in results]
    assert confidences == sorted(confidences, reverse=True)


def test_multiple_categories_when_signals_overlap(clf: FailureClassifier) -> None:
    log = "TypeError in worker. HTTP 503 timeout from upstream."
    results = clf.classify(log)
    categories = {category for category, _ in results}
    assert "CODE" in categories
    assert "TRANSIENT" in categories


def test_confidence_capped_at_one(clf: FailureClassifier) -> None:
    log = "timeout connection reset 503 504 retry exhausted"
    scores = dict(clf.classify(log))
    assert scores["TRANSIENT"] == 1.0
