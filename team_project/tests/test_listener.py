"""Tests for dag_triage Airflow listener hooks."""

from __future__ import annotations

from unittest import mock

from helpers import MockTaskLogReader
from dag_triage.listener import DagTriageListener
from dag_triage.log_tail import LogTailResult
from dag_triage.log_tail_store import get_log_tail_store


def _failed_result(mock_ti: mock.MagicMock) -> LogTailResult:
    return LogTailResult(
        text="tail from failure",
        line_count=1,
        requested_lines=200,
        dag_id=mock_ti.dag_id,
        task_id=mock_ti.task_id,
        run_id=mock_ti.run_id,
        map_index=mock_ti.map_index,
        try_number=mock_ti.try_number,
        state="failed",
    )


@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
@mock.patch("dag_triage.listener.fetch_log_tail")
def test_on_failed_captures_tail(
    mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    mock_fetch.return_value = _failed_result(mock_ti)
    listener = DagTriageListener()

    listener.on_task_instance_failed(None, mock_ti, RuntimeError("boom"))

    mock_fetch.assert_called_once_with(mock_ti)
    cached = get_log_tail_store().get(mock_ti)
    assert cached is not None
    assert cached.text == "tail from failure"


@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
@mock.patch("dag_triage.listener.fetch_log_tail")
def test_on_skipped_captures_tail(
    mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    mock_ti.state = mock.Mock(value="skipped")
    mock_fetch.return_value = _failed_result(mock_ti)
    listener = DagTriageListener()

    listener.on_task_instance_skipped(None, mock_ti)

    mock_fetch.assert_called_once_with(mock_ti)
    assert get_log_tail_store().get(mock_ti) is not None


@mock.patch("dag_triage.listener.capture_on_success", return_value=False)
@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
@mock.patch("dag_triage.listener.fetch_log_tail")
def test_on_success_skipped_when_capture_disabled(
    mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    _mock_capture: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    listener = DagTriageListener()

    listener.on_task_instance_success(None, mock_ti)

    mock_fetch.assert_not_called()
    assert get_log_tail_store().get(mock_ti) is None


@mock.patch("dag_triage.listener.capture_on_success", return_value=True)
@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
@mock.patch("dag_triage.listener.fetch_log_tail")
def test_on_success_captures_when_enabled(
    mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    _mock_capture: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    mock_fetch.return_value = _failed_result(mock_ti)
    listener = DagTriageListener()

    listener.on_task_instance_success(None, mock_ti)

    mock_fetch.assert_called_once_with(mock_ti)


@mock.patch("dag_triage.listener.is_triage_enabled", return_value=False)
@mock.patch("dag_triage.listener.fetch_log_tail")
def test_listener_disabled_does_not_capture(
    mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    listener = DagTriageListener()

    listener.on_task_instance_failed(None, mock_ti, "error")

    mock_fetch.assert_not_called()
    assert get_log_tail_store().get(mock_ti) is None


@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
@mock.patch("dag_triage.listener.fetch_log_tail", side_effect=RuntimeError("unexpected"))
def test_listener_fetch_exception_stores_error_result(
    _mock_fetch: mock.MagicMock,
    _mock_enabled: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    listener = DagTriageListener()

    listener.on_task_instance_failed(None, mock_ti, "error")

    cached = get_log_tail_store().get(mock_ti)
    assert cached is not None
    assert not cached.ok
    assert "unexpected" in cached.error


@mock.patch("dag_triage.config.get_log_tail_lines", return_value=200)
@mock.patch("dag_triage.listener.is_triage_enabled", return_value=True)
def test_listener_integration_with_log_reader(
    _mock_enabled: mock.MagicMock,
    _mock_lines: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    """Listener stores a real tail produced by fetch_log_tail + mock reader."""
    from dag_triage.log_tail import fetch_log_tail

    reader = MockTaskLogReader(["ERROR - Task failed", "AttributeError: boom"])
    listener = DagTriageListener()

    with mock.patch(
        "dag_triage.listener.fetch_log_tail",
        side_effect=lambda ti: fetch_log_tail(ti, log_reader=reader),
    ):
        listener.on_task_instance_failed(None, mock_ti, RuntimeError("boom"))

    cached = get_log_tail_store().get(mock_ti)
    assert cached is not None
    assert cached.ok
    assert "AttributeError" in cached.text
