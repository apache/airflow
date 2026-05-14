"""Heuristic failure classifier for Airflow task-instance logs."""

from __future__ import annotations

import re

# Each entry is (compiled_pattern, confidence_weight).
# Multiple matches in the same category accumulate; total is capped at 1.0.
_RULES: dict[str, list[tuple[re.Pattern[str], float]]] = {
    "TRANSIENT": [
        (re.compile(r"timeout", re.IGNORECASE), 0.4),
        (re.compile(r"connection\s+reset", re.IGNORECASE), 0.5),
        (re.compile(r"\b503\b"), 0.4),
        (re.compile(r"\b504\b"), 0.4),
        (re.compile(r"retry\s+exhausted", re.IGNORECASE), 0.5),
    ],
    "DATA_QUALITY": [
        (re.compile(r"null\s+value", re.IGNORECASE), 0.4),
        (re.compile(r"schema\s+mismatch", re.IGNORECASE), 0.5),
        (re.compile(r"validation\s+error", re.IGNORECASE), 0.4),
        (re.compile(r"type\s+mismatch", re.IGNORECASE), 0.4),
    ],
    "RESOURCE": [
        (re.compile(r"OutOfMemory", re.IGNORECASE), 0.5),
        (re.compile(r"OOMKilled", re.IGNORECASE), 0.6),
        (re.compile(r"disk\s+full", re.IGNORECASE), 0.5),
        (re.compile(r"memory\s+limit", re.IGNORECASE), 0.4),
    ],
    "CODE": [
        (re.compile(r"\bTypeError\b"), 0.4),
        (re.compile(r"\bKeyError\b"), 0.4),
        (re.compile(r"\bAttributeError\b"), 0.4),
        (re.compile(r"\bNameError\b"), 0.4),
        (re.compile(r"\bImportError\b"), 0.4),
    ],
    "EXTERNAL_DEPENDENCY": [
        (re.compile(r"\bDNS\b", re.IGNORECASE), 0.4),
        (re.compile(r"host\s+unreachable", re.IGNORECASE), 0.5),
        (re.compile(r"certificate", re.IGNORECASE), 0.4),
        (re.compile(r"\bSSL\b", re.IGNORECASE), 0.4),
        (re.compile(r"\brefused\b", re.IGNORECASE), 0.3),
    ],
}


class FailureClassifier:
    """Classify an Airflow task-instance log into ranked failure categories.

    Returns a ranked list of (category, confidence) tuples sorted by
    confidence descending. Categories with zero signal are omitted.
    A single label is never asserted; callers should inspect the full
    ranked list and apply their own threshold.
    """

    def classify(self, log_text: str) -> list[tuple[str, float]]:
        """Return ranked (category, confidence) pairs for *log_text*.

        Parameters
        ----------
        log_text:
            Raw or partially normalised Airflow task-instance log text.

        Returns
        -------
        list[tuple[str, float]]
            Categories with non-zero confidence, sorted highest first.
            Empty list when no rule fires.
        """
        scores: dict[str, float] = {}
        for category, rules in _RULES.items():
            total = sum(weight for pattern, weight in rules if pattern.search(log_text))
            if total > 0:
                scores[category] = min(total, 1.0)

        return sorted(scores.items(), key=lambda item: item[1], reverse=True)
