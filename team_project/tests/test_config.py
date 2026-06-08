"""Tests for dag_triage configuration helpers."""

from __future__ import annotations

from unittest import mock

from dag_triage.config import (
    DEFAULT_LOG_TAIL_LINES,
    capture_on_success,
    get_log_tail_lines,
    is_triage_enabled,
)


@mock.patch("airflow.configuration.conf.getint", return_value=500)
def test_get_log_tail_lines_from_conf(mock_getint: mock.MagicMock) -> None:
    assert get_log_tail_lines() == 500
    mock_getint.assert_called_once_with("triage", "log_tail_lines", fallback=DEFAULT_LOG_TAIL_LINES)


@mock.patch("airflow.configuration.conf.getint", return_value=DEFAULT_LOG_TAIL_LINES)
def test_get_log_tail_lines_default(mock_getint: mock.MagicMock) -> None:
    assert get_log_tail_lines() == DEFAULT_LOG_TAIL_LINES
    mock_getint.assert_called_once_with("triage", "log_tail_lines", fallback=DEFAULT_LOG_TAIL_LINES)


@mock.patch("airflow.configuration.conf.getboolean", return_value=False)
def test_is_triage_enabled(mock_getboolean: mock.MagicMock) -> None:
    assert is_triage_enabled() is False
    mock_getboolean.assert_called_once_with("triage", "enabled", fallback=True)


@mock.patch("airflow.configuration.conf.getboolean", return_value=True)
def test_capture_on_success(mock_getboolean: mock.MagicMock) -> None:
    assert capture_on_success() is True
    mock_getboolean.assert_called_once_with("triage", "capture_on_success", fallback=False)
