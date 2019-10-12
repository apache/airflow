# -*- coding: utf-8 -*-
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

import imp
import inspect
import os
import re
from typing import Any, List

import pkg_resources

from airflow import settings
from airflow.models.baseoperator import BaseOperatorLink
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log

import_errors = {}


class AirflowPluginException(Exception):
    pass


class AirflowPlugin:
    name = None  # type: str
    operators = []  # type: List[Any]
    sensors = []  # type: List[Any]
    hooks = []  # type: List[Any]
    executors = []  # type: List[Any]
    macros = []  # type: List[Any]
    admin_views = []  # type: List[Any]
    flask_blueprints = []  # type: List[Any]
    menu_links = []  # type: List[Any]
    appbuilder_views = []  # type: List[Any]
    appbuilder_menu_items = []  # type: List[Any]

    # A function that validate the statsd stat name, apply changes
    # to the stat name if necessary and return the transformed stat name.
    #
    # The function should have the following signature:
    # def func_name(stat_name: str) -> str:
    stat_name_handler = None  # type: Any
    global_operator_extra_links = []  # type: List[BaseOperatorLink]

    @classmethod
    def validate(cls):
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

if settings.PLUGINS_FOLDER is None:
    raise AirflowPluginException("Plugins folder is not set")

# Crawl through the plugins folder to find AirflowPlugin derivatives
for root, dirs, files in os.walk(settings.PLUGINS_FOLDER, followlinks=True):
    for f in files:
        try:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue
            mod_name, file_ext = os.path.splitext(
                os.path.split(filepath)[-1])
            if file_ext != '.py':
                continue

            log.debug('Importing plugin module %s', filepath)
            # normalize root path as namespace
            namespace = '_'.join([re.sub(norm_pattern, '__', root), mod_name])

            m = imp.load_source(namespace, filepath)
            for obj in list(m.__dict__.values()):
                if is_valid_plugin(obj, plugins):
                    plugins.append(obj)

        except Exception as e:
            log.exception(e)
            log.error('Failed to import plugin %s', filepath)
            import_errors[filepath] = str(e)

plugins = load_entrypoint_plugins(
    pkg_resources.iter_entry_points('airflow.plugins'),
    plugins
)


def make_module(name, objects):
    log.debug('Creating module %s', name)
    name = name.lower()
    module = imp.new_module(name)
    module._name = name.split('.')[-1]
    module._objects = objects
    module.__dict__.update((o.__name__, o) for o in objects)
    return module


# Plugin components to integrate as modules
operators_modules = []
sensors_modules = []
hooks_modules = []
executors_modules = []
macros_modules = []

# Plugin components to integrate directly
admin_views = []  # type: List[Any]
flask_blueprints = []  # type: List[Any]
menu_links = []  # type: List[Any]
flask_appbuilder_views = []  # type: List[Any]
flask_appbuilder_menu_links = []  # type: List[Any]
stat_name_handler = None  # type: Any
global_operator_extra_links = []  # type: List[Any]

stat_name_handlers = []
for p in plugins:
    operators_modules.append(
        make_module('airflow.operators.' + p.name, p.operators + p.sensors))
    sensors_modules.append(
        make_module('airflow.sensors.' + p.name, p.sensors)
    )
    hooks_modules.append(make_module('airflow.hooks.' + p.name, p.hooks))
    executors_modules.append(
        make_module('airflow.executors.' + p.name, p.executors))
    macros_modules.append(make_module('airflow.macros.' + p.name, p.macros))

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

if len(stat_name_handlers) > 1:
    raise AirflowPluginException(
        'Specified more than one stat_name_handler ({}) '
        'is not allowed.'.format(stat_name_handlers))

stat_name_handler = stat_name_handlers[0] if len(stat_name_handlers) == 1 else None
