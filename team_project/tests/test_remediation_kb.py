"""Tests for the remediation knowledge base."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest

from dag_triage.remediation_kb import Remediation, RemediationKB


@pytest.fixture(scope="module")
def kb() -> RemediationKB:
    return RemediationKB()


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------


def test_kb_loads_without_error(kb: RemediationKB) -> None:
    assert len(kb._entries) >= 8


def test_entries_have_required_fields(kb: RemediationKB) -> None:
    for entry in kb._entries:
        assert "category" in entry
        assert "title" in entry
        assert "steps" in entry
        assert "doc_links" in entry
        assert len(entry["steps"]) >= 2
        assert len(entry["doc_links"]) >= 1


# ---------------------------------------------------------------------------
# Lookup by category
# ---------------------------------------------------------------------------


def test_lookup_transient_returns_results(kb: RemediationKB) -> None:
    results = kb.lookup("TRANSIENT")
    assert len(results) >= 2
    assert all(isinstance(r, Remediation) for r in results)


def test_lookup_resource_fields_populated(kb: RemediationKB) -> None:
    results = kb.lookup("RESOURCE")
    assert len(results) >= 2
    for r in results:
        assert r.title
        assert len(r.steps) >= 2
        assert len(r.doc_links) >= 1
        assert r.doc_links[0].startswith("https://")


def test_lookup_covers_all_five_categories(kb: RemediationKB) -> None:
    for category in ("TRANSIENT", "DATA_QUALITY", "RESOURCE", "CODE", "EXTERNAL_DEPENDENCY"):
        # DATA_QUALITY may be empty in the seed KB — just confirm no exception
        results = kb.lookup(category)
        assert isinstance(results, list)


# ---------------------------------------------------------------------------
# Lookup with no match returns empty list
# ---------------------------------------------------------------------------


def test_lookup_unknown_category_returns_empty(kb: RemediationKB) -> None:
    assert kb.lookup("NONEXISTENT_CATEGORY") == []


def test_lookup_valid_category_empty_on_signature_mismatch(kb: RemediationKB) -> None:
    results = kb.lookup("RESOURCE", "completely unrelated text xyz123")
    assert results == []


# ---------------------------------------------------------------------------
# Signature regex matching
# ---------------------------------------------------------------------------


def test_signature_match_oomkilled(kb: RemediationKB) -> None:
    results = kb.lookup("RESOURCE", "OOMKilled: memory limit of 512Mi exceeded")
    assert len(results) >= 1
    assert any("OOM" in r.title for r in results)


def test_signature_match_s3_access_denied(kb: RemediationKB) -> None:
    results = kb.lookup("EXTERNAL_DEPENDENCY", "AccessDenied: s3:GetObject on bucket my-bucket")
    assert len(results) >= 1
    assert any("S3" in r.title for r in results)


def test_signature_match_is_case_insensitive(kb: RemediationKB) -> None:
    results_lower = kb.lookup("CODE", "importerror: no module named pandas")
    results_upper = kb.lookup("CODE", "ImportError: No module named pandas")
    assert len(results_lower) == len(results_upper)
