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

import importlib
import importlib.machinery
import importlib.util
import inspect
import logging
import os
import sys
import types
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow import settings
from airflow.configuration import conf
from airflow.task.priority_strategy import (
    PriorityWeightStrategy,
    airflow_priority_weight_strategies,
)
from airflow.utils.entry_points import entry_points_with_dist
from airflow.utils.file import find_path_from_directory
from airflow.utils.module_loading import import_string, qualname

if TYPE_CHECKING:
    from airflow.lineage.hook import HookLineageReader

    try:
        import importlib_metadata as metadata
    except ImportError:
        from importlib import metadata  # type: ignore[no-redef]
    from collections.abc import Generator
    from types import ModuleType

    from airflow.listeners.listener import ListenerManager
    from airflow.timetables.base import Timetable

log = logging.getLogger(__name__)

import_errors: dict[str, str] = {}

plugins: list[AirflowPlugin] | None = None
loaded_plugins: set[str] = set()

# Plugin components to integrate as modules
macros_modules: list[Any] | None = None

# Plugin components to integrate directly
admin_views: list[Any] | None = None
flask_blueprints: list[Any] | None = None
fastapi_apps: list[Any] | None = None
fastapi_root_middlewares: list[Any] | None = None
menu_links: list[Any] | None = None
flask_appbuilder_views: list[Any] | None = None
flask_appbuilder_menu_links: list[Any] | None = None
global_operator_extra_links: list[Any] | None = None
operator_extra_links: list[Any] | None = None
registered_operator_link_classes: dict[str, type] | None = None
timetable_classes: dict[str, type[Timetable]] | None = None
hook_lineage_reader_classes: list[type[HookLineageReader]] | None = None
priority_weight_strategy_classes: dict[str, type[PriorityWeightStrategy]] | None = None
"""
Mapping of class names to class of OperatorLinks registered by plugins.

Used by the DAG serialization code to only allow specific classes to be created
during deserialization
"""
PLUGINS_ATTRIBUTES_TO_DUMP = {
    "macros",
    "admin_views",
    "flask_blueprints",
    "fastapi_apps",
    "fastapi_root_middlewares",
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
        self.dist = dist.metadata["Name"]
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


def is_valid_plugin(plugin_obj):
    """
    Check whether a potential object is a subclass of the AirflowPlugin class.

    :param plugin_obj: potential subclass of AirflowPlugin
    :return: Whether or not the obj is a valid subclass of
        AirflowPlugin
    """
    global plugins

    if (
        inspect.isclass(plugin_obj)
        and issubclass(plugin_obj, AirflowPlugin)
        and (plugin_obj is not AirflowPlugin)
    ):
        plugin_obj.validate()
        return plugin_obj not in plugins
    return False


def register_plugin(plugin_instance):
    """
    Start plugin load and register it after success initialization.

    If plugin is already registered, do nothing.

    :param plugin_instance: subclass of AirflowPlugin
    """
    global plugins

    if plugin_instance.name in loaded_plugins:
        return

    loaded_plugins.add(plugin_instance.name)
    plugin_instance.on_load()
    plugins.append(plugin_instance)


def load_entrypoint_plugins():
    """
    Load and register plugins AirflowPlugin subclasses from the entrypoints.

    The entry_point group should be 'airflow.plugins'.
    """
    global import_errors

    log.debug("Loading plugins from entrypoints")

    for entry_point, dist in entry_points_with_dist("airflow.plugins"):
        log.debug("Importing entry_point plugin %s", entry_point.name)
        try:
            plugin_class = entry_point.load()
            if not is_valid_plugin(plugin_class):
                continue

            plugin_instance = plugin_class()
            plugin_instance.source = EntryPointSource(entry_point, dist)
            register_plugin(plugin_instance)
        except Exception as e:
            log.exception("Failed to import plugin %s", entry_point.name)
            import_errors[entry_point.module] = str(e)


def load_plugins_from_plugin_directory():
    """Load and register Airflow Plugins from plugins directory."""
    global import_errors
    log.debug("Loading plugins from directory: %s", settings.PLUGINS_FOLDER)
    files = find_path_from_directory(settings.PLUGINS_FOLDER, ".airflowignore")
    plugin_search_locations: list[tuple[str, Generator[str, None, None]]] = [("", files)]

    if conf.getboolean("core", "LOAD_EXAMPLES"):
        log.debug("Note: Loading plugins from examples as well: %s", settings.PLUGINS_FOLDER)
        from airflow.example_dags import plugins

        example_plugins_folder = next(iter(plugins.__path__))
        example_files = find_path_from_directory(example_plugins_folder, ".airflowignore")
        plugin_search_locations.append((plugins.__name__, example_files))

    for module_prefix, plugin_files in plugin_search_locations:
        for file_path in plugin_files:
            path = Path(file_path)
            if not path.is_file() or path.suffix != ".py":
                continue
            mod_name = f"{module_prefix}.{path.stem}" if module_prefix else path.stem

            try:
                loader = importlib.machinery.SourceFileLoader(mod_name, file_path)
                spec = importlib.util.spec_from_loader(mod_name, loader)
                mod = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = mod
                loader.exec_module(mod)

                for mod_attr_value in (m for m in mod.__dict__.values() if is_valid_plugin(m)):
                    plugin_instance = mod_attr_value()
                    plugin_instance.source = PluginsDirectorySource(file_path)
                    register_plugin(plugin_instance)
            except Exception as e:
                log.exception("Failed to import plugin %s", file_path)
                import_errors[file_path] = str(e)


def load_providers_plugins():
    from airflow.providers_manager import ProvidersManager

    log.debug("Loading plugins from providers")
    providers_manager = ProvidersManager()
    providers_manager.initialize_providers_plugins()
    for plugin in providers_manager.plugins:
        log.debug("Importing plugin %s from class %s", plugin.name, plugin.plugin_class)

        try:
            plugin_instance = import_string(plugin.plugin_class)
            if is_valid_plugin(plugin_instance):
                register_plugin(plugin_instance)
            else:
                log.warning("Plugin %s is not a valid plugin", plugin.name)
        except ImportError:
            log.exception("Failed to load plugin %s from class name %s", plugin.name, plugin.plugin_class)


def make_module(name: str, objects: list[Any]):
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


def ensure_plugins_loaded():
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    from airflow.stats import Stats

    global plugins

    if plugins is not None:
        log.debug("Plugins are already loaded. Skipping.")
        return

    if not settings.PLUGINS_FOLDER:
        raise ValueError("Plugins folder is not set")

    log.debug("Loading plugins")

    with Stats.timer() as timer:
        plugins = []

        load_plugins_from_plugin_directory()
        load_entrypoint_plugins()

        if not settings.LAZY_LOAD_PROVIDERS:
            load_providers_plugins()

    if plugins:
        log.debug("Loading %d plugin(s) took %.2f seconds", len(plugins), timer.duration)


def initialize_flask_plugins():
    """Collect extension points for WEB UI."""
    global plugins
    global flask_blueprints
    global flask_appbuilder_views
    global flask_appbuilder_menu_links

    if (
        flask_blueprints is not None
        and flask_appbuilder_views is not None
        and flask_appbuilder_menu_links is not None
    ):
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize Web UI plugin")

    flask_blueprints = []
    flask_appbuilder_views = []
    flask_appbuilder_menu_links = []

    for plugin in plugins:
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


def initialize_fastapi_plugins():
    """Collect extension points for the API."""
    global plugins
    global fastapi_apps
    global fastapi_root_middlewares

    if fastapi_apps is not None and fastapi_root_middlewares is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize FastAPI plugins")

    fastapi_apps = []
    fastapi_root_middlewares = []

    for plugin in plugins:
        fastapi_apps.extend(plugin.fastapi_apps)
        fastapi_root_middlewares.extend(plugin.fastapi_root_middlewares)


def initialize_extra_operators_links_plugins():
    """Create modules for loaded extension from extra operators links plugins."""
    global global_operator_extra_links
    global operator_extra_links
    global registered_operator_link_classes

    if (
        global_operator_extra_links is not None
        and operator_extra_links is not None
        and registered_operator_link_classes is not None
    ):
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize extra operators links plugins")

    global_operator_extra_links = []
    operator_extra_links = []
    registered_operator_link_classes = {}

    for plugin in plugins:
        global_operator_extra_links.extend(plugin.global_operator_extra_links)
        operator_extra_links.extend(list(plugin.operator_extra_links))

        registered_operator_link_classes.update(
            {qualname(link.__class__): link.__class__ for link in plugin.operator_extra_links}
        )


def initialize_timetables_plugins():
    """Collect timetable classes registered by plugins."""
    global timetable_classes

    if timetable_classes is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize extra timetables plugins")

    timetable_classes = {
        qualname(timetable_class): timetable_class
        for plugin in plugins
        for timetable_class in plugin.timetables
    }


def initialize_hook_lineage_readers_plugins():
    """Collect hook lineage reader classes registered by plugins."""
    global hook_lineage_reader_classes

    if hook_lineage_reader_classes is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize hook lineage readers plugins")

    hook_lineage_reader_classes = []
    for plugin in plugins:
        hook_lineage_reader_classes.extend(plugin.hook_lineage_readers)


def integrate_macros_plugins() -> None:
    """Integrates macro plugins."""
    global plugins
    global macros_modules

    from airflow import macros

    if macros_modules is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Integrate DAG plugins")

    macros_modules = []

    for plugin in plugins:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        macros_module = make_module(f"airflow.macros.{plugin.name}", plugin.macros)

        if macros_module:
            macros_modules.append(macros_module)
            sys.modules[macros_module.__name__] = macros_module
            # Register the newly created module on airflow.macros such that it
            # can be accessed when rendering templates.
            setattr(macros, plugin.name, macros_module)


def integrate_listener_plugins(listener_manager: ListenerManager) -> None:
    """Add listeners from plugins."""
    global plugins

    ensure_plugins_loaded()

    if plugins:
        for plugin in plugins:
            if plugin.name is None:
                raise AirflowPluginException("Invalid plugin name")

            for listener in plugin.listeners:
                listener_manager.add_listener(listener)


def get_plugin_info(attrs_to_dump: Iterable[str] | None = None) -> list[dict[str, Any]]:
    """
    Dump plugins attributes.

    :param attrs_to_dump: A list of plugin attributes to dump
    """
    ensure_plugins_loaded()
    integrate_macros_plugins()
    initialize_flask_plugins()
    initialize_fastapi_plugins()
    initialize_extra_operators_links_plugins()
    if not attrs_to_dump:
        attrs_to_dump = PLUGINS_ATTRIBUTES_TO_DUMP
    plugins_info = []
    if plugins:
        for plugin in plugins:
            info: dict[str, Any] = {"name": plugin.name}
            for attr in attrs_to_dump:
                if attr in ("global_operator_extra_links", "operator_extra_links"):
                    info[attr] = [f"<{qualname(d.__class__)} object>" for d in getattr(plugin, attr)]
                elif attr in ("macros", "timetables", "priority_weight_strategies"):
                    info[attr] = [qualname(d) for d in getattr(plugin, attr)]
                elif attr == "listeners":
                    # listeners may be modules or class instances
                    info[attr] = [
                        d.__name__ if inspect.ismodule(d) else qualname(d) for d in getattr(plugin, attr)
                    ]
                elif attr == "appbuilder_views":
                    info[attr] = [
                        {**d, "view": qualname(d["view"].__class__) if "view" in d else None}
                        for d in getattr(plugin, attr)
                    ]
                elif attr == "flask_blueprints":
                    info[attr] = [
                        f"<{qualname(d.__class__)}: name={d.name!r} import_name={d.import_name!r}>"
                        for d in getattr(plugin, attr)
                    ]
                elif attr == "fastapi_apps":
                    info[attr] = [
                        {**d, "app": qualname(d["app"].__class__) if "app" in d else None}
                        for d in getattr(plugin, attr)
                    ]
                elif attr == "fastapi_root_middlewares":
                    # remove args and kwargs from plugin info to hide potentially sensitive info.
                    info[attr] = [
                        {
                            k: (v if k != "middleware" else qualname(middleware_dict["middleware"]))
                            for k, v in middleware_dict.items()
                            if k not in ("args", "kwargs")
                        }
                        for middleware_dict in getattr(plugin, attr)
                    ]
                else:
                    info[attr] = getattr(plugin, attr)
            plugins_info.append(info)
    return plugins_info


def initialize_priority_weight_strategy_plugins():
    """Collect priority weight strategy classes registered by plugins."""
    global priority_weight_strategy_classes

    if priority_weight_strategy_classes is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize extra priority weight strategy plugins")

    plugins_priority_weight_strategy_classes = {
        qualname(priority_weight_strategy_class): priority_weight_strategy_class
        for plugin in plugins
        for priority_weight_strategy_class in plugin.priority_weight_strategies
    }
    priority_weight_strategy_classes = {
        **airflow_priority_weight_strategies,
        **plugins_priority_weight_strategy_classes,
    }
