"""Configuration helpers for the dag_triage plugin."""

from __future__ import annotations

DEFAULT_LOG_TAIL_LINES = 200


def get_log_tail_lines() -> int:
    """Return the configured number of log lines to tail (default 200).

    Set in ``airflow.cfg``::

        [triage]
        log_tail_lines = 200
        enabled = true
        capture_on_success = false
    """
    from airflow.configuration import conf

    return conf.getint("triage", "log_tail_lines", fallback=DEFAULT_LOG_TAIL_LINES)


def is_triage_enabled() -> bool:
    """Return whether the dag_triage listener is active."""
    from airflow.configuration import conf

    return conf.getboolean("triage", "enabled", fallback=True)


def capture_on_success() -> bool:
    """Return whether log tails should be captured for successful tasks."""
    from airflow.configuration import conf

    return conf.getboolean("triage", "capture_on_success", fallback=False)
