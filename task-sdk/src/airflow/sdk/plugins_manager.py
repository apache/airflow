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
"""SDK wrapper for plugins manager."""

from __future__ import annotations

import logging
from functools import cache
from typing import TYPE_CHECKING

from airflow import settings
from airflow.observability.stats import Stats
from airflow.providers_manager import ProvidersManager
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk._shared.plugins_manager import (
    AirflowPlugin,
    _load_entrypoint_plugins,
    _load_plugins_from_plugin_directory,
    integrate_listener_plugins as _integrate_listener_plugins,
    integrate_macros_plugins as _integrate_macros_plugins,
    is_valid_plugin,
)
from airflow.sdk.configuration import conf

if TYPE_CHECKING:
    from airflow.listeners.listener import ListenerManager

log = logging.getLogger(__name__)


def _load_providers_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """Load plugins from providers."""
    log.debug("Loading plugins from providers")
    providers_manager = ProvidersManager()
    providers_manager.initialize_providers_plugins()

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    for plugin in providers_manager.plugins:
        log.debug("Importing plugin %s from class %s", plugin.name, plugin.plugin_class)

        try:
            plugin_instance = import_string(plugin.plugin_class)
            if is_valid_plugin(plugin_instance):
                plugins.append(plugin_instance)
            else:
                log.warning("Plugin %s is not a valid plugin", plugin.name)
        except ImportError:
            log.exception("Failed to load plugin %s from class name %s", plugin.name, plugin.plugin_class)
    return plugins, import_errors


@cache
def _get_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    if not settings.PLUGINS_FOLDER:
        raise ValueError("Plugins folder is not set")

    log.debug("Loading plugins")

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    loaded_plugins: set[str | None] = set()

    def __register_plugins(plugin_instances: list[AirflowPlugin], errors: dict[str, str]) -> None:
        for plugin_instance in plugin_instances:
            if plugin_instance.name in loaded_plugins:
                return

            loaded_plugins.add(plugin_instance.name)
            try:
                plugin_instance.on_load()
                plugins.append(plugin_instance)
            except Exception as e:
                log.exception("Failed to load plugin %s", plugin_instance.name)
                name = str(plugin_instance.source) if plugin_instance.source else plugin_instance.name or ""
                import_errors[name] = str(e)
        import_errors.update(errors)

    with Stats.timer() as timer:
        load_examples = conf.getboolean("core", "LOAD_EXAMPLES")
        __register_plugins(
            *_load_plugins_from_plugin_directory(
                plugins_folder=settings.PLUGINS_FOLDER,
                load_examples=load_examples,
                example_plugins_module="airflow.example_dags.plugins" if load_examples else None,
            )
        )
        __register_plugins(*_load_entrypoint_plugins())

        if not settings.LAZY_LOAD_PROVIDERS:
            __register_plugins(*_load_providers_plugins())

    log.debug("Loading %d plugin(s) took %.2f seconds", len(plugins), timer.duration)
    return plugins, import_errors


@cache
def integrate_macros_plugins() -> None:
    """Integrates macro plugins."""
    from airflow.sdk.execution_time import macros

    plugins, _ = _get_plugins()
    _integrate_macros_plugins(
        target_macros_module=macros,
        macros_module_name_prefix="airflow.sdk.execution_time.macros",
        plugins=plugins,
    )


def integrate_listener_plugins(listener_manager: ListenerManager) -> None:
    """Add listeners from plugins."""
    plugins, _ = _get_plugins()
    _integrate_listener_plugins(listener_manager, plugins=plugins)
