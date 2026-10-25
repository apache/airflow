from __future__ import annotations

from dataclasses import dataclass


@dataclass
class DagParseWarningRecord:
    dag_id: str
    message: str
    warning_type: str = "user:dynamic_dag_failure"
