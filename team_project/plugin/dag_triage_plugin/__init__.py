# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
DAG Triage Assistant — Airflow plugin entry point.

Registers a FastAPI sub-app under ``/plugins/dag-triage/`` that exposes
AI-assisted failure triage for Airflow task instances.
"""

from __future__ import annotations

try:
    from airflow.plugins_manager import AirflowPlugin
except ImportError:  # allow importing without a full Airflow install (tests, demo)
    AirflowPlugin = object  # type: ignore[assignment,misc]

from dag_triage_plugin.api import create_app


class DagTriagePlugin(AirflowPlugin):  # type: ignore[misc]
    """Airflow plugin that surfaces triage analysis on the Task Instance view."""

    name = "dag_triage"

    fastapi_apps = [
        {
            "app": create_app(),
            "url_prefix": "/plugins/dag-triage",
            "name": "DAG Triage Assistant",
        }
    ]
