"""Tests for the log tail in-memory store."""

from __future__ import annotations

from unittest import mock

from helpers import MockTaskLogReader
from dag_triage.log_tail import LogTailResult
from dag_triage.log_tail_store import LogTailStore, get_task_log_tail


def _sample_result(mock_ti: mock.MagicMock, *, text: str = "cached tail") -> LogTailResult:
    return LogTailResult(
        text=text,
        line_count=1,
        requested_lines=200,
        dag_id=mock_ti.dag_id,
        task_id=mock_ti.task_id,
        run_id=mock_ti.run_id,
        map_index=mock_ti.map_index,
        try_number=mock_ti.try_number,
        state="failed",
    )


def test_put_and_get(mock_ti: mock.MagicMock) -> None:
    store = LogTailStore()
    result = _sample_result(mock_ti)

    store.put(mock_ti, result)

    assert store.get(mock_ti) == result


@mock.patch("dag_triage.log_tail_store.fetch_log_tail")
def test_get_or_fetch_uses_cache(
    mock_fetch: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    store = LogTailStore()
    cached = _sample_result(mock_ti, text="from cache")
    store.put(mock_ti, cached)

    result = store.get_or_fetch(mock_ti)

    assert result == cached
    mock_fetch.assert_not_called()


@mock.patch("dag_triage.log_tail_store.fetch_log_tail")
def test_get_or_fetch_refresh_bypasses_cache(
    mock_fetch: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    store = LogTailStore()
    store.put(mock_ti, _sample_result(mock_ti, text="stale"))
    fresh = _sample_result(mock_ti, text="fresh")
    mock_fetch.return_value = fresh

    result = store.get_or_fetch(mock_ti, refresh=True)

    assert result == fresh
    mock_fetch.assert_called_once_with(mock_ti, tail_lines=None)


@mock.patch("dag_triage.log_tail_store.fetch_log_tail")
def test_get_or_fetch_fetches_on_miss(
    mock_fetch: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    store = LogTailStore()
    fetched = _sample_result(mock_ti, text="fetched")
    mock_fetch.return_value = fetched

    result = store.get_or_fetch(mock_ti, tail_lines=50)

    assert result == fetched
    mock_fetch.assert_called_once_with(mock_ti, tail_lines=50)
    assert store.get(mock_ti) == fetched


@mock.patch("dag_triage.log_tail_store.get_log_tail_store")
@mock.patch("dag_triage.log_tail_store.fetch_log_tail")
def test_get_task_log_tail_delegates_to_store(
    mock_fetch: mock.MagicMock,
    mock_get_store: mock.MagicMock,
    mock_ti: mock.MagicMock,
) -> None:
    store = mock.MagicMock()
    mock_get_store.return_value = store
    expected = _sample_result(mock_ti)
    store.get_or_fetch.return_value = expected

    result = get_task_log_tail(mock_ti, refresh=True, tail_lines=25)

    assert result == expected
    store.get_or_fetch.assert_called_once_with(mock_ti, refresh=True, tail_lines=25)
    mock_fetch.assert_not_called()
