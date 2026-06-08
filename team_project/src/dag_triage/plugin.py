"""Airflow plugin registration for dag_triage."""

from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from dag_triage.api import triage_app
from dag_triage.listener import get_dag_triage_listener

_TRIAGE_URL_PREFIX = "/triage-panel"


class DagTriagePlugin(AirflowPlugin):
    """AI-assisted Dag failure triage — log tail capture and Task Instance panel."""

    name = "dag_triage"
    listeners = [get_dag_triage_listener()]
    fastapi_apps = [
        {
            "name": "dag_triage_panel",
            "app": triage_app,
            "url_prefix": _TRIAGE_URL_PREFIX,
        }
    ]
