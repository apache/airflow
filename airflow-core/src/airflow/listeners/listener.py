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

from airflow._shared.listeners.listener import ListenerManager
from airflow._shared.listeners.spec import lifecycle, taskinstance
from airflow.listeners.spec import asset, dagrun, importerrors
from airflow.plugins_manager import integrate_listener_plugins


@cache
def get_listener_manager() -> ListenerManager:
    """
    Get a listener manager for Airflow core.

    Registers the following listeners:
    - lifecycle: on_starting, before_stopping
    - dagrun: on_dag_run_running, on_dag_run_success, on_dag_run_failed
    - taskinstance: on_task_instance_running, on_task_instance_success, etc.
    - asset: on_asset_created, on_asset_changed, etc.
    - importerrors: on_new_dag_import_error, on_existing_dag_import_error
    """
    _listener_manager = ListenerManager()

    _listener_manager.add_hookspecs(lifecycle)
    _listener_manager.add_hookspecs(dagrun)
    _listener_manager.add_hookspecs(taskinstance)
    _listener_manager.add_hookspecs(asset)
    _listener_manager.add_hookspecs(importerrors)

    integrate_listener_plugins(_listener_manager)
    return _listener_manager


__all__ = ["get_listener_manager", "ListenerManager"]
