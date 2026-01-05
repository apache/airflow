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

import importlib.machinery
import importlib.util
import inspect
import logging
import os
import sys
import types
from collections.abc import Iterable
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow import settings
from airflow._shared.module_loading import entry_points_with_dist, import_string, qualname
from airflow.configuration import conf
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    airflow_priority_weight_strategies,
)
from airflow.utils.file import find_path_from_directory

if TYPE_CHECKING:
    from airflow.lineage.hook import HookLineageReader

    if sys.version_info >= (3, 12):
        from importlib import metadata
    else:
        import importlib_metadata as metadata
    from collections.abc import Generator
    from types import ModuleType

    from airflow.listeners.listener import ListenerManager
    from airflow.timetables.base import Timetable

log = logging.getLogger(__name__)


class AirflowPluginSource:
    """Class used to define an AirflowPluginSource."""

    def __str__(self):
        raise NotImplementedError

    def __html__(self):
        raise NotImplementedError


class PluginsDirectorySource(AirflowPluginSource):
    """Class used to define Plugins loaded from Plugins Directory."""

    def __init__(self, path):
        self.path = os.path.relpath(path, settings.PLUGINS_FOLDER)

    def __str__(self):
        return f"$PLUGINS_FOLDER/{self.path}"

    def __html__(self):
        return f"<em>$PLUGINS_FOLDER/</em>{self.path}"


class EntryPointSource(AirflowPluginSource):
    """Class used to define Plugins loaded from entrypoint."""

    def __init__(self, entrypoint: metadata.EntryPoint, dist: metadata.Distribution):
        self.dist = dist.metadata["Name"]  # type: ignore[index]
        self.version = dist.version
        self.entrypoint = str(entrypoint)

    def __str__(self):
        return f"{self.dist}=={self.version}: {self.entrypoint}"

    def __html__(self):
        return f"<em>{self.dist}=={self.version}:</em> {self.entrypoint}"


class AirflowPluginException(Exception):
    """Exception when loading plugin."""


class AirflowPlugin:
    """Class used to define AirflowPlugin."""

    name: str | None = None
    source: AirflowPluginSource | None = None
    macros: list[Any] = []
    admin_views: list[Any] = []
    flask_blueprints: list[Any] = []
    fastapi_apps: list[Any] = []
    fastapi_root_middlewares: list[Any] = []
    external_views: list[Any] = []
    react_apps: list[Any] = []
    menu_links: list[Any] = []
    appbuilder_views: list[Any] = []
    appbuilder_menu_items: list[Any] = []

    # A list of global operator extra links that can redirect users to
    # external systems. These extra links will be available on the
    # task page in the form of buttons.
    #
    # Note: the global operator extra link can be overridden at each
    # operator level.
    global_operator_extra_links: list[Any] = []

    # A list of operator extra links to override or add operator links
    # to existing Airflow Operators.
    # These extra links will be available on the task page in form of
    # buttons.
    operator_extra_links: list[Any] = []

    # A list of timetable classes that can be used for DAG scheduling.
    timetables: list[type[Timetable]] = []

    # A list of listeners that can be used for tracking task and DAG states.
    listeners: list[ModuleType | object] = []

    # A list of hook lineage reader classes that can be used for reading lineage information from a hook.
    hook_lineage_readers: list[type[HookLineageReader]] = []

    # A list of priority weight strategy classes that can be used for calculating tasks weight priority.
    priority_weight_strategies: list[type[PriorityWeightStrategy]] = []

    @classmethod
    def validate(cls):
        """Validate if plugin has a name."""
        if not cls.name:
            raise AirflowPluginException("Your plugin needs a name.")

    @classmethod
    def on_load(cls, *args, **kwargs):
        """
        Execute when the plugin is loaded; This method is only called once during runtime.

        :param args: If future arguments are passed in on call.
        :param kwargs: If future arguments are passed in on call.
        """


def is_valid_plugin(plugin_obj) -> bool:
    """
    Check whether a potential object is a subclass of the AirflowPlugin class.

    :param plugin_obj: potential subclass of AirflowPlugin
    :return: Whether or not the obj is a valid subclass of
        AirflowPlugin
    """
    if (
        inspect.isclass(plugin_obj)
        and issubclass(plugin_obj, AirflowPlugin)
        and (plugin_obj is not AirflowPlugin)
    ):
        plugin_obj.validate()
        return True
    return False


def _load_entrypoint_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """
    Load and register plugins AirflowPlugin subclasses from the entrypoints.

    The entry_point group should be 'airflow.plugins'.
    """
    log.debug("Loading plugins from entrypoints")

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    for entry_point, dist in entry_points_with_dist("airflow.plugins"):
        log.debug("Importing entry_point plugin %s", entry_point.name)
        try:
            plugin_class = entry_point.load()
            if not is_valid_plugin(plugin_class):
                continue

            plugin_instance: AirflowPlugin = plugin_class()
            plugin_instance.source = EntryPointSource(entry_point, dist)
            plugins.append(plugin_instance)
        except Exception as e:
            log.exception("Failed to import plugin %s", entry_point.name)
            import_errors[entry_point.module] = str(e)
    return plugins, import_errors


def _load_plugins_from_plugin_directory() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """Load and register Airflow Plugins from plugins directory."""
    if settings.PLUGINS_FOLDER is None:
        raise ValueError("Plugins folder is not set")
    log.debug("Loading plugins from directory: %s", settings.PLUGINS_FOLDER)
    files = find_path_from_directory(settings.PLUGINS_FOLDER, ".airflowignore")
    plugin_search_locations: list[tuple[str, Generator[str, None, None]]] = [("", files)]

    if conf.getboolean("core", "LOAD_EXAMPLES"):
        log.debug("Note: Loading plugins from examples as well: %s", settings.PLUGINS_FOLDER)
        from airflow.example_dags import plugins as example_plugins

        example_plugins_folder = next(iter(example_plugins.__path__))
        example_files = find_path_from_directory(example_plugins_folder, ".airflowignore")
        plugin_search_locations.append((example_plugins.__name__, example_files))

    plugins: list[AirflowPlugin] = []
    import_errors: dict[str, str] = {}
    for module_prefix, plugin_files in plugin_search_locations:
        for file_path in plugin_files:
            path = Path(file_path)
            if not path.is_file() or path.suffix != ".py":
                continue
            mod_name = f"{module_prefix}.{path.stem}" if module_prefix else path.stem

            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, file_path)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                if not spec:
                    log.error("Could not load spec for module %s at %s", mod_name, file_path)
                    continue
                mod = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = mod
                loader.exec_module(mod)

                for mod_attr_value in (m for m in mod.__dict__.values() if is_valid_plugin(m)):
                    plugin_instance: AirflowPlugin = mod_attr_value()
                    plugin_instance.source = PluginsDirectorySource(file_path)
                    plugins.append(plugin_instance)
            except Exception as e:
                log.exception("Failed to import plugin %s", file_path)
                import_errors[file_path] = str(e)
    return plugins, import_errors


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


def make_module(name: str, objects: list[Any]) -> ModuleType | None:
    """Create new module."""
    if not objects:
        return None
    log.debug("Creating module %s", name)
    name = name.lower()
    module = types.ModuleType(name)
    module._name = name.split(".")[-1]  # type: ignore
    module._objects = objects  # type: ignore
    module.__dict__.update((o.__name__, o) for o in objects)
    return module


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
        __register_plugins(*_load_plugins_from_plugin_directory())
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
    from airflow.sdk.execution_time import macros

    log.debug("Integrate Macros plugins")

    for plugin in _get_plugins()[0]:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        macros_module = make_module(f"airflow.sdk.execution_time.macros.{plugin.name}", plugin.macros)

        if macros_module:
            sys.modules[macros_module.__name__] = macros_module
            # Register the newly created module on airflow.macros such that it
            # can be accessed when rendering templates.
            setattr(macros, plugin.name, macros_module)


def integrate_listener_plugins(listener_manager: ListenerManager) -> None:
    """Add listeners from plugins."""
    for plugin in _get_plugins()[0]:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        for listener in plugin.listeners:
            listener_manager.add_listener(listener)


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
