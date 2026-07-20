#
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
from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING

from airflow._shared.listeners.listener import ListenerManager
from airflow._shared.listeners.spec import lifecycle, taskinstance
from airflow.configuration import conf
from airflow.listeners.spec import asset, dagrun, importerrors
from airflow.plugins_manager import integrate_listener_plugins

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@cache
def _build_listener_manager(team_name: str | None) -> ListenerManager:
    _listener_manager = ListenerManager()

    _listener_manager.add_hookspecs(lifecycle)
    _listener_manager.add_hookspecs(dagrun)
    _listener_manager.add_hookspecs(taskinstance)
    _listener_manager.add_hookspecs(asset)
    _listener_manager.add_hookspecs(importerrors)

    integrate_listener_plugins(_listener_manager, team_name=team_name)
    return _listener_manager


def get_listener_manager(team_name: str | None = None) -> ListenerManager:
    """
    Get a listener manager for Airflow core.

    Registers the following listeners:
    - lifecycle: on_starting, before_stopping
    - dagrun: on_dag_run_running, on_dag_run_success, on_dag_run_failed
    - taskinstance: on_task_instance_running, on_task_instance_success, etc.
    - asset: on_asset_created, on_asset_changed, etc.
    - importerrors: on_new_dag_import_error, on_existing_dag_import_error

    :param team_name: In multi-team mode, the team whose (and the global) plugin
        listeners this manager should contain. ``None`` yields a manager with only
        global-plugin listeners (used by team-agnostic events such as lifecycle,
        asset, and import-error hooks). When multi-team mode is disabled this
        argument is ignored and the manager holds every plugin's listeners.

    Calls are cached per ``team_name``. The single positional delegation to
    ``_build_listener_manager`` guarantees that ``get_listener_manager()``,
    ``get_listener_manager(None)`` and ``get_listener_manager(team_name=None)`` all
    return the identical (global) manager instance rather than distinct cache
    entries.
    """
    return _build_listener_manager(team_name)


def get_listener_manager_for_dag(dag_id: str, session: Session | None = None) -> ListenerManager:
    """
    Get the listener manager scoped to the team that owns ``dag_id``.

    When multi-team mode is disabled this returns the single manager holding all
    listeners. Otherwise it resolves the Dag's owning team and returns a manager
    containing the global listeners plus that team's listeners.
    """
    if not conf.getboolean("core", "multi_team"):
        return get_listener_manager()

    from airflow.models.dag import DagModel

    team_name = DagModel.get_team_name(dag_id, session=session) if session else DagModel.get_team_name(dag_id)
    return get_listener_manager(team_name)


__all__ = ["get_listener_manager", "get_listener_manager_for_dag", "ListenerManager"]
