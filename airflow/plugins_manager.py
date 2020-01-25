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
# noinspection PyDeprecation
import importlib
import inspect
import os
import re
import sys
import types
from typing import Any, Callable, Dict, List, Optional, Set, Type

import pkg_resources

from airflow import settings
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

import_errors = {}


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

    # A function that validate the statsd stat name, apply changes
    # to the stat name if necessary and return the transformed stat name.
    #
    # The function should have the following signature:
    # def func_name(stat_name: str) -> str:
    stat_name_handler: Optional[Callable[[str], str]] = None

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


def load_entrypoint_plugins(entry_points, airflow_plugins):
    """
    Load AirflowPlugin subclasses from the entrypoints
    provided. The entry_point group should be 'airflow.plugins'.

    :param entry_points: A collection of entrypoints to search for plugins
    :type entry_points: Generator[setuptools.EntryPoint, None, None]
    :param airflow_plugins: A collection of existing airflow plugins to
        ensure we don't load duplicates
    :type airflow_plugins: list[type[airflow.plugins_manager.AirflowPlugin]]
    :rtype: list[airflow.plugins_manager.AirflowPlugin]
    """
    for entry_point in entry_points:
        log.debug('Importing entry_point plugin %s', entry_point.name)
        plugin_obj = entry_point.load()
        if is_valid_plugin(plugin_obj, airflow_plugins):
            if callable(getattr(plugin_obj, 'on_load', None)):
                plugin_obj.on_load()
                airflow_plugins.append(plugin_obj)
    return airflow_plugins


def register_inbuilt_operator_links() -> None:
    """
    Register all the Operators Links that are already defined for the operators
    in the "airflow" project. Example: QDSLink (Operator Link for Qubole Operator)

    This is required to populate the "whitelist" of allowed classes when deserializing operator links
    """
    inbuilt_operator_links: Set[Type] = set()

    try:
        from airflow.gcp.operators.bigquery import BigQueryConsoleLink, BigQueryConsoleIndexableLink  # noqa E501 # pylint: disable=R0401,line-too-long
        inbuilt_operator_links.update([BigQueryConsoleLink, BigQueryConsoleIndexableLink])
    except ImportError:
        pass

    try:
        from airflow.providers.qubole.operators.qubole import QDSLink   # pylint: disable=R0401
        inbuilt_operator_links.update([QDSLink])
    except ImportError:
        pass

    registered_operator_link_classes.update({
        "{}.{}".format(link.__module__, link.__name__): link
        for link in inbuilt_operator_links
    })


def is_valid_plugin(plugin_obj, existing_plugins):
    """
    Check whether a potential object is a subclass of
    the AirflowPlugin class.

    :param plugin_obj: potential subclass of AirflowPlugin
    :param existing_plugins: Existing list of AirflowPlugin subclasses
    :return: Whether or not the obj is a valid subclass of
        AirflowPlugin
    """
    if (
        inspect.isclass(plugin_obj) and
        issubclass(plugin_obj, AirflowPlugin) and
        (plugin_obj is not AirflowPlugin)
    ):
        plugin_obj.validate()
        return plugin_obj not in existing_plugins
    return False


plugins = []  # type: List[AirflowPlugin]

norm_pattern = re.compile(r'[/|.]')

if not settings.PLUGINS_FOLDER:
    raise ValueError("Plugins folder is not set")

# Crawl through the plugins folder to find AirflowPlugin derivatives
for root, dirs, files in os.walk(settings.PLUGINS_FOLDER, followlinks=True):
    for f in files:
        filepath = os.path.join(root, f)
        try:
            if not os.path.isfile(filepath):
                continue
            mod_name, file_ext = os.path.splitext(
                os.path.split(filepath)[-1])
            if file_ext != '.py':
                continue

            log.debug('Importing plugin module %s', filepath)
            # normalize root path as namespace
            namespace = '_'.join([re.sub(norm_pattern, '__', root), mod_name])

            loader = importlib.machinery.SourceFileLoader(mod_name, filepath)
            spec = importlib.util.spec_from_loader(mod_name, loader)
            m = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = m
            loader.exec_module(m)
            for obj in list(m.__dict__.values()):
                if is_valid_plugin(obj, plugins):
                    plugins.append(obj)
        except Exception as e:  # pylint: disable=broad-except
            log.exception(e)
            path = filepath or str(f)
            log.error('Failed to import plugin %s', path)
            import_errors[path] = str(e)

plugins = load_entrypoint_plugins(
    pkg_resources.iter_entry_points('airflow.plugins'),
    plugins
)


# pylint: disable=protected-access
# noinspection Mypy,PyTypeHints
def make_module(name: str, objects: List[Any]):
    """Creates new module."""
    log.debug('Creating module %s', name)
    name = name.lower()
    module = types.ModuleType(name)
    module._name = name.split('.')[-1]  # type: ignore
    module._objects = objects           # type: ignore
    module.__dict__.update((o.__name__, o) for o in objects)
    return module
# pylint: enable=protected-access


# Plugin components to integrate as modules
operators_modules = []
sensors_modules = []
hooks_modules = []
executors_modules = []
macros_modules = []

# Plugin components to integrate directly
admin_views: List[Any] = []
flask_blueprints: List[Any] = []
menu_links: List[Any] = []
flask_appbuilder_views: List[Any] = []
flask_appbuilder_menu_links: List[Any] = []
stat_name_handler: Any = None
global_operator_extra_links: List[Any] = []
operator_extra_links: List[Any] = []
registered_operator_link_classes: Dict[str, Type] = {}
"""Mapping of class names to class of OperatorLinks registered by plugins.

Used by the DAG serialization code to only allow specific classes to be created
during deserialization
"""

stat_name_handlers = []
for p in plugins:
    if not p.name:
        raise AirflowPluginException("Plugin name is missing.")
    plugin_name: str = p.name
    operators_modules.append(
        make_module('airflow.operators.' + plugin_name, p.operators + p.sensors))
    sensors_modules.append(
        make_module('airflow.sensors.' + plugin_name, p.sensors)
    )
    hooks_modules.append(make_module('airflow.hooks.' + plugin_name, p.hooks))
    executors_modules.append(
        make_module('airflow.executors.' + plugin_name, p.executors))
    macros_modules.append(make_module('airflow.macros.' + plugin_name, p.macros))

    admin_views.extend(p.admin_views)
    menu_links.extend(p.menu_links)
    flask_appbuilder_views.extend(p.appbuilder_views)
    flask_appbuilder_menu_links.extend(p.appbuilder_menu_items)
    flask_blueprints.extend([{
        'name': p.name,
        'blueprint': bp
    } for bp in p.flask_blueprints])
    if p.stat_name_handler:
        stat_name_handlers.append(p.stat_name_handler)
    global_operator_extra_links.extend(p.global_operator_extra_links)
    operator_extra_links.extend(list(p.operator_extra_links))

    registered_operator_link_classes.update({
        "{}.{}".format(link.__class__.__module__,
                       link.__class__.__name__): link.__class__
        for link in p.operator_extra_links
    })

if len(stat_name_handlers) > 1:
    raise AirflowPluginException(
        'Specified more than one stat_name_handler ({}) '
        'is not allowed.'.format(stat_name_handlers))

stat_name_handler = stat_name_handlers[0] if len(stat_name_handlers) == 1 else None


def integrate_operator_plugins() -> None:
    """Integrate operators plugins to the context"""
    for operators_module in operators_modules:
        sys.modules[operators_module.__name__] = operators_module
        # noinspection PyProtectedMember
        globals()[operators_module._name] = operators_module  # pylint: disable=protected-access


def integrate_sensor_plugins() -> None:
    """Integrate sensor plugins to the context"""
    for sensors_module in sensors_modules:
        sys.modules[sensors_module.__name__] = sensors_module
        # noinspection PyProtectedMember
        globals()[sensors_module._name] = sensors_module  # pylint: disable=protected-access


def integrate_hook_plugins() -> None:
    """Integrate hook plugins to the context"""
    for hooks_module in hooks_modules:
        sys.modules[hooks_module.__name__] = hooks_module
        # noinspection PyProtectedMember
        globals()[hooks_module._name] = hooks_module  # pylint: disable=protected-access


def integrate_executor_plugins() -> None:
    """Integrate executor plugins to the context."""
    for executors_module in executors_modules:
        sys.modules[executors_module.__name__] = executors_module
        # noinspection PyProtectedMember
        globals()[executors_module._name] = executors_module  # pylint: disable=protected-access


def integrate_macro_plugins() -> None:
    """Integrate macro plugins to the context"""
    for macros_module in macros_modules:
        sys.modules[macros_module.__name__] = macros_module
        # noinspection PyProtectedMember
        globals()[macros_module._name] = macros_module  # pylint: disable=protected-access


def integrate_plugins() -> None:
    """Integrates all types of plugins."""
    integrate_operator_plugins()
    integrate_sensor_plugins()
    integrate_hook_plugins()
    integrate_executor_plugins()
    integrate_macro_plugins()
    register_inbuilt_operator_links()
