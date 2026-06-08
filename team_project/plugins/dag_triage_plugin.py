"""Drop-in Airflow plugin entry point for dag_triage.

Ensure ``team_project/src`` is on ``PYTHONPATH``, then symlink or copy this
file into your Airflow plugins folder::

    export PYTHONPATH="/path/to/team_project/src:$PYTHONPATH"
    ln -s /path/to/team_project/plugins/dag_triage_plugin.py "$AIRFLOW_HOME/plugins/"

Alternatively, install the package editable from ``team_project/``::

    pip install -e team_project/
"""

from __future__ import annotations

from dag_triage.plugin import DagTriagePlugin

__all__ = ["DagTriagePlugin"]
