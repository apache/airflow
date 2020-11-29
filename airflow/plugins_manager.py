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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import object
import imp
import inspect
import logging
import os
import re
import sys
import warnings
from typing import Any, Dict, List, Type

from six import with_metaclass

try:
    import importlib.metadata as importlib_metadata
except ImportError:
    import importlib_metadata

from airflow import settings
from airflow.models.baseoperator import BaseOperatorLink

log = logging.getLogger(__name__)

import_errors = {}


PY37 = sys.version_info >= (3, 7)


class AirflowPluginException(Exception):
    pass


class _MetaPluginClass(type):
    def __new__(cls, name, bases, props):
        if props.get('operators', []) or props.get('sensors', []):
            warnings.warn(
                "Registering operators or sensors in plugins is deprecated -- these should be treated like "
                "'plain' python modules, and imported normally in DAGs.\n"
                "\n"
                "Airflow 2.0 has removed the ability to register these types in plugins. See "
                "<http://airflow.apache.org/docs/stable/howto/custom-operator.html>.",
                category=FutureWarning,
                stacklevel=2,
            )

        return super(_MetaPluginClass, cls).__new__(cls, name, bases, props)


class AirflowPlugin(with_metaclass(_MetaPluginClass, object)):
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

    # A list of global operator extra links that can redirect users to
    # external systems. These extra links will be available on the
    # task page in the form of buttons.
    #
    # Note: the global operator extra link can be overridden at each
    # operator level.
    global_operator_extra_links = []  # type: List[BaseOperatorLink]

    # A list of operator extra links to override or add operator links
    # to existing Airflow Operators.
    # These extra links will be available on the task page in form of
    # buttons.
    operator_extra_links = []  # type: List[BaseOperatorLink]

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


def entry_points_with_dist(group):
    """
    Return EntryPoint objects of the given group, along with the distribution information.

    This is like the ``entry_points()`` function from importlib.metadata,
    except it also returns the distribution the entry_point was loaded from.

    :param group: FIlter results to only this entrypoint group
    :return: Generator of (EntryPoint, Distribution) objects for the specified groups
    """
    for dist in importlib_metadata.distributions():
        for e in dist.entry_points:
            if e.group != group:
                continue
            yield (e, dist)


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
    global import_errors  # pylint: disable=global-statement
    for entry_point, dist in entry_points:
        log.debug('Importing entry_point plugin %s', entry_point.name)
        try:
            plugin_obj = entry_point.load()
            plugin_obj.__usable_import_name = entry_point.module
            if not is_valid_plugin(plugin_obj, airflow_plugins):
                continue

            if callable(getattr(plugin_obj, 'on_load', None)):
                plugin_obj.on_load()

                airflow_plugins.append(plugin_obj)
        except Exception as e:  # pylint: disable=broad-except
            log.exception("Failed to import plugin %s", entry_point.name)
            import_errors[entry_point.module_name] = str(e)
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

            if mod_name == "__init__":
                compat_import_name = root
            else:
                compat_import_name = os.path.join(root, mod_name)

            compat_import_name = os.path.relpath(
                compat_import_name,
                settings.PLUGINS_FOLDER,
            ).replace(os.sep, '.')

            # normalize root path as namespace
            namespace = '_'.join([re.sub(norm_pattern, '__', root), mod_name])

            m = imp.load_source(namespace, filepath)
            for obj in list(m.__dict__.values()):
                if is_valid_plugin(obj, plugins):
                    obj.__usable_import_name = compat_import_name
                    plugins.append(obj)

        except Exception as e:
            log.exception(e)
            log.error('Failed to import plugin %s', filepath)
            import_errors[filepath] = str(e)

plugins = load_entrypoint_plugins(
    entry_points_with_dist('airflow.plugins'),
    plugins
)


def make_deprecated_module(kind, plugin, objects=None):
    name = 'airflow.{}.{}'.format(kind, plugin.name)
    module = imp.new_module(name)
    module._name = name.split('.')[-1]
    if objects is None:
        objects = getattr(plugin, kind)
    module._objects = objects
    objects = {o.__name__: o for o in objects}

    def __getattr__(attrname):
        """Get attribute."""
        if attrname not in objects:
            raise AttributeError("module '{}' has no attribute '{}'".format(name, attrname))

        stacklevel = 2 if PY37 else 3

        obj = objects[attrname]
        # Use __qualname__ where we have it for Py 3.3+
        obj_name = getattr(obj, '__qualname__', obj.__name__)

        # Work out what the "correct" import name should be
        if obj.__module__ == plugin.__module__:
            # Class is defined in the plugin
            correct_import_name = '.'.join((plugin.__usable_import_name, obj_name))
        else:
            # Class was imported from somewhere else, just direct user to use that instead
            correct_import_name = '.'.join((obj.__module__, obj_name))

        warnings.warn(
            "Importing '{}' from under 'airflow.{}.*' has been deprecated and should be directly "
            "imported as '{}' instead.\n"
            "\n"
            "Support for importing from within the airflow namespace for plugins will be dropped entirely "
            "in Airflow 2.0. See <http://airflow.apache.org/docs/stable/howto/custom-operator.html>.".format(
                attrname, kind, correct_import_name
            ),
            category=FutureWarning,
            stacklevel=stacklevel
        )
        return obj

    def __dir__():
        return objects.keys()

    module.__getattr__ = __getattr__
    module.__dir__ = __dir__

    return module


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
global_operator_extra_links = []  # type: List[BaseOperatorLink]
operator_extra_links = []  # type: List[BaseOperatorLink]

registered_operator_link_classes = {}   # type: Dict[str, Type]
"""Mapping of class names to class of OperatorLinks registered by plugins.

Used by the DAG serialization code to only allow specific classes to be created
during deserialization
"""

for p in plugins:
    operators_modules.append(
        make_deprecated_module('operators', p, p.operators + p.sensors))
    sensors_modules.append(
        make_deprecated_module('sensors', p)
    )
    hooks_modules.append(
        make_deprecated_module('hooks', p)
    )
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
    global_operator_extra_links.extend(p.global_operator_extra_links)

    operator_extra_links.extend([ope for ope in p.operator_extra_links])

    registered_operator_link_classes.update({
        "{}.{}".format(link.__class__.__module__,
                       link.__class__.__name__): link.__class__
        for link in p.operator_extra_links
    })
