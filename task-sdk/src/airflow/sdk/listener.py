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

from airflow.sdk._shared.listeners.listener import ListenerManager
from airflow.sdk._shared.listeners.spec import lifecycle, taskinstance
from airflow.sdk.plugins_manager import integrate_listener_plugins


@cache
def _build_listener_manager(team_name: str | None) -> ListenerManager:
    _listener_manager = ListenerManager()

    _listener_manager.add_hookspecs(lifecycle)
    _listener_manager.add_hookspecs(taskinstance)

    integrate_listener_plugins(_listener_manager, team_name=team_name)  # type: ignore[arg-type]
    return _listener_manager


def get_listener_manager(team_name: str | None = None) -> ListenerManager:
    """
    Get a listener manager for task sdk.

    Registers the following listeners:
    - lifecycle: on_starting, before_stopping
    - taskinstance: on_task_instance_running, on_task_instance_success, etc.

    :param team_name: The team of the task being executed, as provided by the server
        in the task instance context. The manager contains the global plugin
        listeners plus that team's listeners. ``None`` (a teamless task, or multi-team
        disabled) yields a manager with only the global plugin listeners. The worker
        deliberately relies on this server-provided value rather than reading
        ``core.multi_team`` from its own (possibly incomplete) configuration.

    Calls are cached per ``team_name``. The single positional delegation to
    ``_build_listener_manager`` guarantees that ``get_listener_manager()``,
    ``get_listener_manager(None)`` and ``get_listener_manager(team_name=None)`` all
    return the identical (global) manager instance.
    """
    return _build_listener_manager(team_name)


__all__ = ["get_listener_manager", "ListenerManager"]
