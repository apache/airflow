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
import importlib
import importlib.machinery
import importlib.util
import inspect
import logging
import os
import sys
import types
from typing import Any, Dict, List, Optional, Type

import pkg_resources

from airflow import settings
from airflow.utils.file import find_path_from_directory

log = logging.getLogger(__name__)

import_errors: Dict[str, str] = {}

plugins = None  # type: Optional[List[AirflowPlugin]]

# Plugin components to integrate as modules
operators_modules: Optional[List[Any]] = None
sensors_modules: Optional[List[Any]] = None
hooks_modules: Optional[List[Any]] = None
macros_modules: Optional[List[Any]] = None
executors_modules: Optional[List[Any]] = None

# Plugin components to integrate directly
admin_views: Optional[List[Any]] = None
flask_blueprints: Optional[List[Any]] = None
menu_links: Optional[List[Any]] = None
flask_appbuilder_views: Optional[List[Any]] = None
flask_appbuilder_menu_links: Optional[List[Any]] = None
global_operator_extra_links: Optional[List[Any]] = None
operator_extra_links: Optional[List[Any]] = None
registered_operator_link_classes: Optional[Dict[str, Type]] = None
"""Mapping of class names to class of OperatorLinks registered by plugins.

Used by the DAG serialization code to only allow specific classes to be created
during deserialization
"""


class AirflowPluginException(Exception):
    """Exception when loading plugin."""


class AirflowPlugin:
    """Class used to define AirflowPlugin."""
    name: Optional[str] = None
    operators: List[Any] = []
    sensors: List[Any] = []
    hooks: List[Any] = []
    executors: List[Any] = []
    macros: List[Any] = []
    admin_views: List[Any] = []
    flask_blueprints: List[Any] = []
    menu_links: List[Any] = []
    appbuilder_views: List[Any] = []
    appbuilder_menu_items: List[Any] = []

    # A list of global operator extra links that can redirect users to
    # external systems. These extra links will be available on the
    # task page in the form of buttons.
    #
    # Note: the global operator extra link can be overridden at each
    # operator level.
    global_operator_extra_links: List[Any] = []

    # A list of operator extra links to override or add operator links
    # to existing Airflow Operators.
    # These extra links will be available on the task page in form of
    # buttons.
    operator_extra_links: List[Any] = []

    @classmethod
    def validate(cls):
        """Validates that plugin has a name."""
        if not cls.name:
            raise AirflowPluginException("Your plugin needs a name.")

    @classmethod
    def on_load(cls, *args, **kwargs):
        """
        Executed when the plugin is loaded.
        This method is only called once during runtime.

        :param args: If future arguments are passed in on call.
        :param kwargs: If future arguments are passed in on call.
        """


def is_valid_plugin(plugin_obj):
    """
    Check whether a potential object is a subclass of
    the AirflowPlugin class.

    :param plugin_obj: potential subclass of AirflowPlugin
    :return: Whether or not the obj is a valid subclass of
        AirflowPlugin
    """
    global plugins  # pylint: disable=global-statement

    if (
        inspect.isclass(plugin_obj) and
        issubclass(plugin_obj, AirflowPlugin) and
        (plugin_obj is not AirflowPlugin)
    ):
        plugin_obj.validate()
        return plugin_obj not in plugins
    return False


def load_entrypoint_plugins():
    """
    Load and register plugins AirflowPlugin subclasses from the entrypoints.
    The entry_point group should be 'airflow.plugins'.
    """
    global import_errors  # pylint: disable=global-statement
    global plugins  # pylint: disable=global-statement

    entry_points = pkg_resources.iter_entry_points('airflow.plugins')

    log.debug("Loading plugins from entrypoints")

    for entry_point in entry_points:  # pylint: disable=too-many-nested-blocks
        log.debug('Importing entry_point plugin %s', entry_point.name)
        try:
            plugin_class = entry_point.load()
            if is_valid_plugin(plugin_class):
                plugin_instance = plugin_class()
                if callable(getattr(plugin_instance, 'on_load', None)):
                    plugin_instance.on_load()
                    plugins.append(plugin_instance)
        except Exception as e:  # pylint: disable=broad-except
            log.exception("Failed to import plugin %s", entry_point.name)
            import_errors[entry_point.module_name] = str(e)


def load_plugins_from_plugin_directory():
    """
    Load and register Airflow Plugins from plugins directory
    """
    global import_errors  # pylint: disable=global-statement
    global plugins  # pylint: disable=global-statement
    log.debug("Loading plugins from directory: %s", settings.PLUGINS_FOLDER)

    for file_path in find_path_from_directory(
            settings.PLUGINS_FOLDER, ".airflowignore"):

        if not os.path.isfile(file_path):
            continue
        mod_name, file_ext = os.path.splitext(os.path.split(file_path)[-1])
        if file_ext != '.py':
            continue

        try:
            loader = importlib.machinery.SourceFileLoader(mod_name, file_path)
            spec = importlib.util.spec_from_loader(mod_name, loader)
            mod = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = mod
            loader.exec_module(mod)
            log.debug('Importing plugin module %s', file_path)

            for mod_attr_value in (m for m in mod.__dict__.values() if is_valid_plugin(m)):
                plugin_instance = mod_attr_value()
                plugins.append(plugin_instance)

        except Exception as e:  # pylint: disable=broad-except
            log.exception(e)
            log.error('Failed to import plugin %s', file_path)
            import_errors[file_path] = str(e)


# pylint: disable=protected-access
def make_module(name: str, objects: List[Any]):
    """Creates new module."""
    if not objects:
        return None
    log.debug('Creating module %s', name)
    name = name.lower()
    module = types.ModuleType(name)
    module._name = name.split('.')[-1]  # type: ignore
    module._objects = objects           # type: ignore
    module.__dict__.update((o.__name__, o) for o in objects)
    return module
# pylint: enable=protected-access


def ensure_plugins_loaded():
    """
    Load plugins from plugins directory and entrypoints.

    Plugins are only loaded if they have not been previously loaded.
    """
    global plugins  # pylint: disable=global-statement

    if plugins is not None:
        log.debug("Plugins are already loaded. Skipping.")
        return

    if not settings.PLUGINS_FOLDER:
        raise ValueError("Plugins folder is not set")

    log.debug("Loading plugins")

    plugins = []

    load_plugins_from_plugin_directory()
    load_entrypoint_plugins()


def initialize_web_ui_plugins():
    """Collect extension points for WEB UI"""
    # pylint: disable=global-statement
    global plugins

    global admin_views
    global flask_blueprints
    global menu_links
    global flask_appbuilder_views
    global flask_appbuilder_menu_links
    # pylint: enable=global-statement

    if admin_views is not None and \
            flask_blueprints is not None and \
            menu_links is not None and \
            flask_appbuilder_views is not None and \
            flask_appbuilder_menu_links is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Initialize Web UI plugin")

    admin_views = []
    flask_blueprints = []
    menu_links = []
    flask_appbuilder_views = []
    flask_appbuilder_menu_links = []

    for plugin in plugins:
        admin_views.extend(plugin.admin_views)
        menu_links.extend(plugin.menu_links)
        flask_appbuilder_views.extend(plugin.appbuilder_views)
        flask_appbuilder_menu_links.extend(plugin.appbuilder_menu_items)
        flask_blueprints.extend([{
            'name': plugin.name,
            'blueprint': bp
        } for bp in plugin.flask_blueprints])

        if (admin_views and not flask_appbuilder_views) or (menu_links and not flask_appbuilder_menu_links):
            log.warning(
                "Plugin \'%s\' may not be compatible with the current Airflow version. "
                "Please contact the author of the plugin.",
                plugin.name
            )


def initialize_extra_operators_links_plugins():
    """Creates modules for loaded extension from extra operators links plugins"""
    # pylint: disable=global-statement
    global global_operator_extra_links
    global operator_extra_links
    global registered_operator_link_classes
    # pylint: enable=global-statement

    if global_operator_extra_links is not None and \
            operator_extra_links is not None and \
            registered_operator_link_classes is not None:
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

        registered_operator_link_classes.update({
            "{}.{}".format(link.__class__.__module__,
                           link.__class__.__name__): link.__class__
            for link in plugin.operator_extra_links
        })


def integrate_executor_plugins() -> None:
    """Integrate executor plugins to the context."""
    # pylint: disable=global-statement
    global plugins
    global executors_modules
    # pylint: enable=global-statement

    if executors_modules is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Integrate executor plugins")

    executors_modules = []
    for plugin in plugins:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")
        plugin_name: str = plugin.name

        executors_module = make_module('airflow.executors.' + plugin_name, plugin.executors)
        if executors_module:
            executors_modules.append(executors_module)
            sys.modules[executors_module.__name__] = executors_module  # pylint: disable=no-member


def integrate_dag_plugins() -> None:
    """Integrates operator, sensor, hook, macro plugins."""
    # pylint: disable=global-statement
    global plugins
    global operators_modules
    global sensors_modules
    global hooks_modules
    global macros_modules
    # pylint: enable=global-statement

    if operators_modules is not None and \
            sensors_modules is not None and \
            hooks_modules is not None and \
            macros_modules is not None:
        return

    ensure_plugins_loaded()

    if plugins is None:
        raise AirflowPluginException("Can't load plugins.")

    log.debug("Integrate DAG plugins")

    operators_modules = []
    sensors_modules = []
    hooks_modules = []
    macros_modules = []

    for plugin in plugins:
        if plugin.name is None:
            raise AirflowPluginException("Invalid plugin name")

        operators_module = make_module(f'airflow.operators.{plugin.name}', plugin.operators + plugin.sensors)
        sensors_module = make_module(f'airflow.sensors.{plugin.name}', plugin.sensors)
        hooks_module = make_module(f'airflow.hooks.{plugin.name}', plugin.hooks)
        macros_module = make_module(f'airflow.macros.{plugin.name}', plugin.macros)

        if operators_module:
            operators_modules.append(operators_module)
            sys.modules[operators_module.__name__] = operators_module  # pylint: disable=no-member

        if sensors_module:
            sensors_modules.append(sensors_module)
            sys.modules[sensors_module.__name__] = sensors_module  # pylint: disable=no-member

        if hooks_module:
            hooks_modules.append(hooks_module)
            sys.modules[hooks_module.__name__] = hooks_module  # pylint: disable=no-member

        if macros_module:
            macros_modules.append(macros_module)
            sys.modules[macros_module.__name__] = macros_module  # pylint: disable=no-member
