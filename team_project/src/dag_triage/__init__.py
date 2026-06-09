"""dag_triage — AI-assisted DAG failure triage plugin.

Milestone Mavericks team, CSS 566A, UW Bothell, Spring 2026.
"""

from dag_triage.log_tail import LogTailResult, fetch_log_tail
from dag_triage.log_tail_store import get_task_log_tail

__all__ = ["LogTailResult", "fetch_log_tail", "get_task_log_tail"]
__version__ = "1.0.0"
