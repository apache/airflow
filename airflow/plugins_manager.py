# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from builtins import object
import copy
import imp
import inspect
import logging
import os
import re
import sys

from airflow import configuration
from airflow.models import BaseOperator

class AirflowPluginException(Exception):
    pass


class AirflowPlugin(object):
    name = None
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

    # Defines which function to call per button
    # key is the name of the button, which is a string
    # value is the function to call to get the URL corresponding to the button
    # The function should follow this format:
    # type: (BaseOperator, datetime) -> str
    extra_link_functions = {}

    @classmethod
    def validate(cls):
        if not cls.name:
            raise AirflowPluginException("Your plugin needs a name.")


plugins_folder = configuration.get('core', 'plugins_folder')
if not plugins_folder:
    plugins_folder = configuration.get('core', 'airflow_home') + '/plugins'
plugins_folder = os.path.expanduser(plugins_folder)

if plugins_folder not in sys.path:
    sys.path.append(plugins_folder)

plugins = []

norm_pattern = re.compile(r'[/|.]')

# Crawl through the plugins folder to find AirflowPlugin derivatives
for root, dirs, files in os.walk(plugins_folder, followlinks=True):
    for f in files:
        try:
            filepath = os.path.join(root, f)
            if not os.path.isfile(filepath):
                continue
            mod_name, file_ext = os.path.splitext(
                os.path.split(filepath)[-1])
            if file_ext != '.py':
                continue

            logging.debug('Importing plugin module ' + filepath)
            # normalize root path as namespace
            namespace = '_'.join([re.sub(norm_pattern, '__', root), mod_name])

            m = imp.load_source(namespace, filepath)
            for obj in list(m.__dict__.values()):
                if (
                        inspect.isclass(obj) and
                        issubclass(obj, AirflowPlugin) and
                        obj is not AirflowPlugin):
                    obj.validate()
                    if obj not in plugins:
                        plugins.append(obj)

        except Exception as e:
            logging.exception(e)
            logging.error('Failed to import plugin ' + filepath)


def make_module(name, objects):
    logging.debug('Creating module ' + name)
    name = name.lower()
    module = imp.new_module(name)
    module._name = name.split('.')[-1]
    module._objects = objects
    module.__dict__.update((o.__name__, o) for o in objects)
    return module

# Plugin components to integrate as modules
operators_modules = []
hooks_modules = []
executors_modules = []
macros_modules = []

# Plugin components to integrate directly
admin_views = []
flask_blueprints = []
menu_links = []

# Extra links used by all operators.
# Key is the link name and value is the function to execute.
extra_link_functions = {}

for p in plugins:
    extra_link_functions.update(p.extra_link_functions)

BaseOperator.extra_link_functions = extra_link_functions

for p in plugins:
    operators_modules.append(
        make_module('airflow.operators.' + p.name, p.operators))
    hooks_modules.append(make_module('airflow.hooks.' + p.name, p.hooks))
    executors_modules.append(
        make_module('airflow.executors.' + p.name, p.executors))
    macros_modules.append(make_module('airflow.macros.' + p.name, p.macros))

    admin_views.extend(p.admin_views)
    flask_blueprints.extend(p.flask_blueprints)
    menu_links.extend(p.menu_links)

# Merge base extra links with operators' own extra links
for operators_module in operators_modules:
    for operator in operators_module._objects:
        if issubclass(operator, BaseOperator):
            operator_extra_link_functions = copy.copy(BaseOperator.extra_link_functions)
            operator_extra_link_functions.update(operator.extra_link_functions)
            operator.extra_link_functions = operator_extra_link_functions
