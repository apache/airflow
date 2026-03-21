# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.context import ExecutionContext, detect_context


def test_execution_context_creation() -> None:
    """Test ExecutionContext creation and accessors."""
    ctx_host = ExecutionContext(environment="host")
    assert ctx_host.is_host()
    assert not ctx_host.is_breeze()

    ctx_breeze = ExecutionContext(environment="breeze-container")
    assert ctx_breeze.is_breeze()
    assert not ctx_breeze.is_host()


def test_detect_context_returns_valid_environment() -> None:
    """Test that detect_context returns a valid ExecutionContext."""
    ctx = detect_context()
    assert isinstance(ctx, ExecutionContext)
    assert ctx.environment in {"host", "breeze-container"}


def test_detect_context_via_env_variable(monkeypatch) -> None:
    """Test detection via AIRFLOW_BREEZE_CONTAINER environment variable."""
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "true")
    ctx = detect_context()
    assert ctx.is_breeze()


def test_detect_context_prefers_env_variable(monkeypatch, tmp_path) -> None:
    """Test that env variable takes priority over file checks."""
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "false")
    # Even if /.dockerenv existed, the env variable should win
    ctx = detect_context()
    assert ctx.is_host()
