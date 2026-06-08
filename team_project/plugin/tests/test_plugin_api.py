"""Tests for the DAG Triage plugin FastAPI sub-application."""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure dag_triage src is importable without a full pip install.
sys.path.insert(0, str(Path(__file__).parents[3] / "src"))

import pytest
from dag_triage_plugin.api import create_app
from fastapi.testclient import TestClient

_LOG_OOM = (
    "[2024-01-01T00:00:01.000+0000] {worker.py:42} ERROR - "
    "Container OOMKilled: memory limit of 512Mi exceeded"
)
_LOG_IMPORT_ERROR = (
    "[2024-01-01T00:00:01.000+0000] {worker.py:42} ERROR - "
    "ImportError: No module named 'pandas'. Check your dependencies."
)
_LOG_TRANSIENT = (
    "[2024-01-01T00:00:01.000+0000] {worker.py:42} ERROR - "
    "Task failed after 30s: connection reset by peer, timeout exceeded"
)
_LOG_CLEAN = "[2024-01-01T00:00:01.000+0000] {worker.py:42} INFO - Task started successfully. All checks passed."


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(create_app())


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def test_health_returns_ok(client: TestClient) -> None:
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["plugin"] == "dag_triage"
    assert "version" in body


# ---------------------------------------------------------------------------
# Panel route
# ---------------------------------------------------------------------------


def test_panel_returns_html(client: TestClient) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    assert "text/html" in resp.headers["content-type"]
    assert "DAG Triage Assistant" in resp.text


# ---------------------------------------------------------------------------
# Triage endpoint — happy paths
# ---------------------------------------------------------------------------


def _triage(client: TestClient, log: str, dag_id: str = "test_dag") -> dict:
    resp = client.post(
        "/api/v1/triage",
        json={
            "dag_id": dag_id,
            "run_id": "run_1",
            "task_id": "task_1",
            "log_content": log,
        },
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def test_triage_classifies_oom_as_resource(client: TestClient) -> None:
    data = _triage(client, _LOG_OOM)
    assert data["failure_category"] == "RESOURCE"
    assert data["confidence"] > 0
    assert len(data["remediations"]) > 0


def test_triage_classifies_import_error_as_code(client: TestClient) -> None:
    data = _triage(client, _LOG_IMPORT_ERROR)
    assert data["failure_category"] == "CODE"


def test_triage_classifies_timeout_as_transient(client: TestClient) -> None:
    data = _triage(client, _LOG_TRANSIENT)
    assert data["failure_category"] == "TRANSIENT"


def test_triage_unrecognised_log_returns_null_category(client: TestClient) -> None:
    data = _triage(client, _LOG_CLEAN)
    assert data["failure_category"] is None
    assert data["confidence"] == 0.0
    assert data["remediations"] == []


# ---------------------------------------------------------------------------
# Response schema contract
# ---------------------------------------------------------------------------


def test_triage_response_echoes_identifiers(client: TestClient) -> None:
    data = _triage(client, _LOG_OOM, dag_id="my_etl")
    assert data["dag_id"] == "my_etl"
    assert data["run_id"] == "run_1"
    assert data["task_id"] == "task_1"


def test_triage_confidence_bounded(client: TestClient) -> None:
    data = _triage(client, _LOG_OOM)
    assert 0.0 <= data["confidence"] <= 1.0


def test_triage_remediation_structure(client: TestClient) -> None:
    data = _triage(client, _LOG_OOM)
    for rem in data["remediations"]:
        assert "title" in rem
        assert isinstance(rem["steps"], list)
        assert isinstance(rem["doc_links"], list)


def test_triage_root_cause_summary_is_nonempty_string(client: TestClient) -> None:
    data = _triage(client, _LOG_OOM)
    assert isinstance(data["root_cause_summary"], str)
    assert len(data["root_cause_summary"]) > 0


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def test_triage_missing_required_field_returns_422(client: TestClient) -> None:
    resp = client.post(
        "/api/v1/triage",
        json={"dag_id": "x", "run_id": "y"},  # missing task_id and log_content
    )
    assert resp.status_code == 422


# ---------------------------------------------------------------------------
# OpenAPI schema is served
# ---------------------------------------------------------------------------


def test_openapi_schema_served(client: TestClient) -> None:
    resp = client.get("/api/v1/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert "paths" in schema
    assert "/api/v1/triage" in schema["paths"]
