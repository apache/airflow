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
"""Manages all plugins."""

from __future__ import annotations

import inspect
import logging
from collections.abc import Iterable
from functools import cache
from typing import TYPE_CHECKING, Any

from airflow import settings
from airflow._shared.module_loading import (
    import_string,
    qualname,
)
from airflow._shared.plugins_manager import (
    AirflowPlugin,
    AirflowPluginSource as AirflowPluginSource,
    PluginsDirectorySource as PluginsDirectorySource,
    _load_entrypoint_plugins,
    _load_plugins_from_plugin_directory,
    is_valid_plugin,
)
from airflow.configuration import conf
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    airflow_priority_weight_strategies,
)

if TYPE_CHECKING:
    from airflow.lineage.hook import HookLineageReader
    from airflow.listeners.listener import ListenerManager
    from airflow.timetables.base import Timetable

log = logging.getLogger(__name__)


def _load_providers_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    from airflow.providers_manager import ProvidersManager

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


def ensure_plugins_loaded() -> None:
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    _get_plugins()


@cache
def _get_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    from airflow.observability.stats import Stats

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
        ignore_file_syntax = conf.get_mandatory_value("core", "DAG_IGNORE_FILE_SYNTAX", fallback="glob")
        __register_plugins(
            *_load_plugins_from_plugin_directory(
                plugins_folder=settings.PLUGINS_FOLDER,
                load_examples=load_examples,
                example_plugins_module="airflow.example_dags.plugins" if load_examples else None,
                ignore_file_syntax=ignore_file_syntax,
            )
        )
        __register_plugins(*_load_entrypoint_plugins())

        if not settings.LAZY_LOAD_PROVIDERS:
            __register_plugins(*_load_providers_plugins())

    log.debug("Loading %d plugin(s) took %.2f seconds", len(plugins), timer.duration)
    return plugins, import_errors


@cache
def _get_ui_plugins() -> tuple[list[Any], list[Any]]:
    """Collect extension points for the UI."""
    log.debug("Initialize UI plugin")

    seen_url_routes: dict[str, str | None] = {}

    external_views: list[Any] = []
    react_apps: list[Any] = []
    for plugin in _get_plugins()[0]:
        external_views_to_remove = []
        react_apps_to_remove = []
        for external_view in plugin.external_views:
            if not isinstance(external_view, dict):
                log.warning(
                    "Plugin '%s' has an external view that is not a dictionary. The view will not be loaded.",
                    plugin.name,
                )
                external_views_to_remove.append(external_view)
                continue
            url_route = external_view.get("url_route")
            if url_route is None:
                continue
            if url_route in seen_url_routes:
                log.warning(
                    "Plugin '%s' has an external view with an URL route '%s' "
                    "that conflicts with another plugin '%s'. The view will not be loaded.",
                    plugin.name,
                    url_route,
                    seen_url_routes[url_route],
                )
                external_views_to_remove.append(external_view)
                continue
            external_views.append(external_view)
            seen_url_routes[url_route] = plugin.name

        for react_app in plugin.react_apps:
            if not isinstance(react_app, dict):
                log.warning(
                    "Plugin '%s' has a React App that is not a dictionary. The React App will not be loaded.",
                    plugin.name,
                )
                react_apps_to_remove.append(react_app)
                continue
            url_route = react_app.get("url_route")
            if url_route is None:
                continue
            if url_route in seen_url_routes:
                log.warning(
                    "Plugin '%s' has a React App with an URL route '%s' "
                    "that conflicts with another plugin '%s'. The React App will not be loaded.",
                    plugin.name,
                    url_route,
                    seen_url_routes[url_route],
                )
                react_apps_to_remove.append(react_app)
                continue
            react_apps.append(react_app)
            seen_url_routes[url_route] = plugin.name

        for item in external_views_to_remove:
            plugin.external_views.remove(item)
        for item in react_apps_to_remove:
            plugin.react_apps.remove(item)
    return external_views, react_apps


@cache
def get_flask_plugins() -> tuple[list[Any], list[Any], list[Any]]:
    """Collect and get flask extension points for WEB UI (legacy)."""
    log.debug("Initialize legacy Web UI plugin")

    flask_appbuilder_views: list[Any] = []
    flask_appbuilder_menu_links: list[Any] = []
    flask_blueprints: list[Any] = []
    for plugin in _get_plugins()[0]:
        flask_appbuilder_views.extend(plugin.appbuilder_views)
        flask_appbuilder_menu_links.extend(plugin.appbuilder_menu_items)
        flask_blueprints.extend([{"name": plugin.name, "blueprint": bp} for bp in plugin.flask_blueprints])

        if (plugin.admin_views and not plugin.appbuilder_views) or (
            plugin.menu_links and not plugin.appbuilder_menu_items
        ):
            log.warning(
                "Plugin '%s' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
                plugin.name,
            )
    return flask_blueprints, flask_appbuilder_views, flask_appbuilder_menu_links


@cache
def get_fastapi_plugins() -> tuple[list[Any], list[Any]]:
    """Collect extension points for the API."""
    log.debug("Initialize FastAPI plugins")

    fastapi_apps: list[Any] = []
    fastapi_root_middlewares: list[Any] = []
    for plugin in _get_plugins()[0]:
        fastapi_apps.extend(plugin.fastapi_apps)
        fastapi_root_middlewares.extend(plugin.fastapi_root_middlewares)
    return fastapi_apps, fastapi_root_middlewares


@cache
def _get_extra_operators_links_plugins() -> tuple[list[Any], list[Any]]:
    """Create and get modules for loaded extension from extra operators links plugins."""
    log.debug("Initialize extra operators links plugins")

    global_operator_extra_links: list[Any] = []
    operator_extra_links: list[Any] = []
    for plugin in _get_plugins()[0]:
        global_operator_extra_links.extend(plugin.global_operator_extra_links)
        operator_extra_links.extend(list(plugin.operator_extra_links))
    return global_operator_extra_links, operator_extra_links


def get_global_operator_extra_links() -> list[Any]:
    """Get global operator extra links registered by plugins."""
    return _get_extra_operators_links_plugins()[0]


def get_operator_extra_links() -> list[Any]:
    """Get operator extra links registered by plugins."""
    return _get_extra_operators_links_plugins()[1]


@cache
def get_timetables_plugins() -> dict[str, type[Timetable]]:
    """Collect and get timetable classes registered by plugins."""
    log.debug("Initialize extra timetables plugins")

    return {
        qualname(timetable_class): timetable_class
        for plugin in _get_plugins()[0]
        for timetable_class in plugin.timetables
    }


@cache
def get_hook_lineage_readers_plugins() -> list[type[HookLineageReader]]:
    """Collect and get hook lineage reader classes registered by plugins."""
    log.debug("Initialize hook lineage readers plugins")
    result: list[type[HookLineageReader]] = []

    for plugin in _get_plugins()[0]:
        result.extend(plugin.hook_lineage_readers)
    return result


@cache
def integrate_macros_plugins() -> None:
    """Integrates macro plugins."""
    from airflow._shared.plugins_manager import (
        integrate_macros_plugins as _integrate_macros_plugins,
    )
    from airflow.sdk.execution_time import macros

    plugins, _ = _get_plugins()
    _integrate_macros_plugins(
        target_macros_module=macros,
        macros_module_name_prefix="airflow.sdk.execution_time.macros",
        plugins=plugins,
    )


def integrate_listener_plugins(listener_manager: ListenerManager) -> None:
    """Add listeners from plugins."""
    from airflow._shared.plugins_manager import (
        integrate_listener_plugins as _integrate_listener_plugins,
    )

    plugins, _ = _get_plugins()
    _integrate_listener_plugins(listener_manager, plugins=plugins)


def get_plugin_info(attrs_to_dump: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """
    Dump plugins attributes.

    :param attrs_to_dump: A list of plugin attributes to dump
    """
    get_flask_plugins()
    get_fastapi_plugins()
    get_global_operator_extra_links()
    get_operator_extra_links()
    _get_ui_plugins()
    if not attrs_to_dump:
        attrs_to_dump = {
            "macros",
            "admin_views",
            "flask_blueprints",
            "fastapi_apps",
            "fastapi_root_middlewares",
            "external_views",
            "react_apps",
            "menu_links",
            "appbuilder_views",
            "appbuilder_menu_items",
            "global_operator_extra_links",
            "operator_extra_links",
            "source",
            "timetables",
            "listeners",
            "priority_weight_strategies",
        }
    plugins_info = []
    for plugin in _get_plugins()[0]:
        info: dict[str, Any] = {"name": plugin.name}
        for attr in attrs_to_dump:
            if attr in ("global_operator_extra_links", "operator_extra_links"):
                info[attr] = [f"<{qualname(d.__class__)} object>" for d in getattr(plugin, attr)]
            elif attr in ("macros", "timetables", "priority_weight_strategies"):
                info[attr] = [qualname(d) for d in getattr(plugin, attr)]
            elif attr == "listeners":
                # listeners may be modules or class instances
                info[attr] = [d.__name__ if inspect.ismodule(d) else qualname(d) for d in plugin.listeners]
            elif attr == "appbuilder_views":
                info[attr] = [
                    {**d, "view": qualname(d["view"].__class__) if "view" in d else None}
                    for d in plugin.appbuilder_views
                ]
            elif attr == "flask_blueprints":
                info[attr] = [
                    f"<{qualname(d.__class__)}: name={d.name!r} import_name={d.import_name!r}>"
                    for d in plugin.flask_blueprints
                ]
            elif attr == "fastapi_apps":
                info[attr] = [
                    {**d, "app": qualname(d["app"].__class__) if "app" in d else None}
                    for d in plugin.fastapi_apps
                ]
            elif attr == "fastapi_root_middlewares":
                # remove args and kwargs from plugin info to hide potentially sensitive info.
                info[attr] = [
                    {
                        k: (v if k != "middleware" else qualname(middleware_dict["middleware"]))
                        for k, v in middleware_dict.items()
                        if k not in ("args", "kwargs")
                    }
                    for middleware_dict in plugin.fastapi_root_middlewares
                ]
            else:
                info[attr] = getattr(plugin, attr)
        plugins_info.append(info)
    return plugins_info


@cache
def get_priority_weight_strategy_plugins() -> dict[str, type[PriorityWeightStrategy]]:
    """Collect and get priority weight strategy classes registered by plugins."""
    log.debug("Initialize extra priority weight strategy plugins")

    plugins_priority_weight_strategy_classes = {
        qualname(priority_weight_strategy_class): priority_weight_strategy_class
        for plugin in _get_plugins()[0]
        for priority_weight_strategy_class in plugin.priority_weight_strategies
    }
    return {
        **airflow_priority_weight_strategies,
        **plugins_priority_weight_strategy_classes,
    }


def get_import_errors() -> dict[str, str]:
    """Get import errors encountered during plugin loading."""
    return _get_plugins()[1]
