"""Airflow plugin registration for dag_triage."""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from dag_triage.listener import get_dag_triage_listener


class DagTriagePlugin(AirflowPlugin):
    """AI-assisted Dag failure triage — log tail capture on task terminal states."""

    name = "dag_triage"
    listeners = [get_dag_triage_listener()]
