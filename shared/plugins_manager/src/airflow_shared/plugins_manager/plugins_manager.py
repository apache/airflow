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
import os
import sys
import types
from pathlib import Path
from .models import (
    AppBuilderMenuItem,
    AppBuilderView,
    ExternalView,
    FastAPIApp,
    FastAPIRootMiddleware,
    ReactApp,
)

if TYPE_CHECKING:
    from collections.abc import Generator
    from types import ModuleType

    from airflow.listeners.listener import ListenerManager
    from airflow.models.baseoperator import BaseOperatorLink
    from airflow.partition_mapper.base import PartitionMapper
    from airflow.task.priority_strategy import PriorityWeightStrategy
    from airflow.timetables.base import Timetable
    from flask import Blueprint
    if sys.version_info >= (3, 12):
        from importlib import metadata
    else:
        import importlib_metadata as metadata
    from collections.abc import Generator
    from types import ModuleType

    from ..listeners.listener import ListenerManager

log = logging.getLogger(__name__)


class AirflowPluginSource:
    """Class used to define an AirflowPluginSource."""

    def __str__(self):
        raise NotImplementedError

    def __html__(self):
        raise NotImplementedError


class PluginsDirectorySource(AirflowPluginSource):
    """Class used to define Plugins loaded from Plugins Directory."""

    def __init__(self, path, plugins_folder: str):
        self.path = os.path.relpath(path, plugins_folder)

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
    macros: list[Callable | Any] = []
    admin_views: list[AppBuilderView | dict[str, Any]] = []
    flask_blueprints: list[Blueprint | Any] = []
    fastapi_apps: list[FastAPIApp | dict[str, Any]] = []
    fastapi_root_middlewares: list[FastAPIRootMiddleware | dict[str, Any]] = []
    external_views: list[ExternalView | dict[str, Any]] = []
    react_apps: list[ReactApp | dict[str, Any]] = []
    menu_links: list[AppBuilderMenuItem | dict[str, Any]] = []
    appbuilder_views: list[AppBuilderView | dict[str, Any]] = []
    appbuilder_menu_items: list[AppBuilderMenuItem | dict[str, Any]] = []

    # A list of global operator extra links that can redirect users to
    # external systems. These extra links will be available on the
    # task page in the form of buttons.
    #
    # Note: the global operator extra link can be overridden at each
    # operator level.
    global_operator_extra_links: list[BaseOperatorLink | Any] = []

    # A list of operator extra links to override or add operator links
    # to existing Airflow Operators.
    #
    # These extra links will be available on the task page in form of
    # buttons.
    operator_extra_links: list[BaseOperatorLink | Any] = []

    # A list of timetable classes that can be used for Dag scheduling.
    timetables: list[type[Timetable] | Any] = []

    # A list of timetable classes that can be used for Dag scheduling.
    partition_mappers: list[type[PartitionMapper] | Any] = []

    # A list of listeners that can be used for tracking task and Dag states.
    listeners: list[ModuleType | object | ListenerManager] = []

    # A list of hook lineage reader classes that can be used for reading lineage information from a hook.
    hook_lineage_readers: list[Any] = []

    # A list of priority weight strategy classes that can be used for calculating tasks weight priority.
    priority_weight_strategies: list[type[PriorityWeightStrategy] | Any] = []

    @classmethod
    def validate(cls):
        """Validate if plugin has a name and valid attributes."""
        if not cls.name:
            raise AirflowPluginException("Your plugin needs a name.")

        validation_errors = []

        def _validate_list(attr_name: str, model_cls: type[BaseModel] | None = None, check_callable: bool = False):
            attr_value = getattr(cls, attr_name)
            if not isinstance(attr_value, list):
                validation_errors.append(f"'{attr_name}' must be a list, not {type(attr_value).__name__}")
                return

            for i, item in enumerate(attr_value):
                if model_cls:
                    if isinstance(item, dict):
                        from pydantic import ValidationError

                        try:
                            model_cls.model_validate(item)
                        except ValidationError as e:
                            validation_errors.append(f"Invalid configuration for '{attr_name}' at index {i}: {e}")
                    elif not isinstance(item, model_cls):
                        validation_errors.append(
                            f"Item index {i} in '{attr_name}' must be dict or {model_cls.__name__}"
                        )
                elif check_callable:
                    if not callable(item):
                        validation_errors.append(f"Item index {i} in '{attr_name}' must be callable")

        # Declarative validation
        _validate_list("fastapi_apps", FastAPIApp)
        _validate_list("fastapi_root_middlewares", FastAPIRootMiddleware)
        _validate_list("external_views", ExternalView)
        _validate_list("react_apps", ReactApp)
        _validate_list("appbuilder_views", AppBuilderView)
        _validate_list("appbuilder_menu_items", AppBuilderMenuItem)
        _validate_list("admin_views", AppBuilderView)
        _validate_list("menu_links", AppBuilderMenuItem)

        _validate_list("macros", check_callable=True)
        for attr in [
            "flask_blueprints",
            "timetables",
            "priority_weight_strategies",
            "global_operator_extra_links",
            "operator_extra_links",
            "listeners",
        ]:
            _validate_list(attr)

        if validation_errors:
            raise AirflowPluginException(f"Plugin '{cls.name}' validation failed:\n" + "\n".join(validation_errors))

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
    if not inspect.isclass(plugin_obj):
        return False

    # Temporarily here, we use a name base checking instead of issubclass() because the shared library
    # is accessed via different symlink paths in core (airflow._shared) and task sdk (airflow.sdk._shared).
    # Python treats these as different modules, so the AirflowPlugin class has different identities in each context.
    # Providers will typically inherit from SDK AirflowPlugin, so using issubclass() would fail when core tries
    # to validate those plugins and provider plugins won't work in airflow core.
    # For now, by validating by class name, we allow plugins defined with either
    # core's or SDK's AirflowPlugin to be loaded.
    is_airflow_plugin = any(
        base.__name__ == "AirflowPlugin" and "plugins_manager" in base.__module__
        for base in plugin_obj.__mro__
    )

    if is_airflow_plugin and plugin_obj.__name__ != "AirflowPlugin":
        plugin_obj.validate()
        return True
    return False


def _load_entrypoint_plugins() -> tuple[list[AirflowPlugin], dict[str, str]]:
    """
    Load and register plugins AirflowPlugin subclasses from the entrypoints.

    The entry_point group should be 'airflow.plugins'.
    """
    from ..module_loading import entry_points_with_dist

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


def _load_plugins_from_plugin_directory(
    plugins_folder: str,
    load_examples: bool = False,
    example_plugins_module: str | None = None,
    ignore_file_syntax: str = "glob",
) -> tuple[list[AirflowPlugin], dict[str, str]]:
    """Load and register Airflow Plugins from plugins directory."""
    import importlib

    from ..module_loading import find_path_from_directory

    if not plugins_folder:
        raise ValueError("Plugins folder is not set")
    log.debug("Loading plugins from directory: %s", plugins_folder)
    files = find_path_from_directory(plugins_folder, ".airflowignore", ignore_file_syntax)
    plugin_search_locations: list[tuple[str, Generator[str, None, None]]] = [("", files)]

    if load_examples:
        log.debug("Note: Loading plugins from examples as well: %s", plugins_folder)
        example_plugins = importlib.import_module(example_plugins_module)
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
                    plugin_instance.source = PluginsDirectorySource(file_path, plugins_folder)
                    plugins.append(plugin_instance)
            except Exception as e:
                log.exception("Failed to import plugin %s", file_path)
                import_errors[file_path] = str(e)
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


def integrate_macros_plugins(
    target_macros_module: ModuleType, macros_module_name_prefix: str, plugins: list[AirflowPlugin]
) -> None:
    """
    Register macros from plugins onto the target macros module.

    For each plugin with macros, creates a submodule and attaches it to
    the target module so macros can be accessed in templates as
    ``{{ macros.plugin_name.macro_func() }}``.
    """
    log.debug("Integrate Macros plugins")

    for plugin in plugins:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        macros_module_instance = make_module(f"{macros_module_name_prefix}.{plugin.name}", plugin.macros)

        if macros_module_instance:
            sys.modules[macros_module_instance.__name__] = macros_module_instance
            # Register the newly created module on the provided macros module
            # so it can be accessed when rendering templates.
            setattr(target_macros_module, plugin.name, macros_module_instance)


def integrate_listener_plugins(listener_manager: ListenerManager, plugins: list[AirflowPlugin]) -> None:
    """
    Register listeners from plugins with the listener manager.

    For each plugin with listeners, registers them with the provided
    ListenerManager.
    """
    for plugin in plugins:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        for listener in plugin.listeners:
            listener_manager.add_listener(listener)
