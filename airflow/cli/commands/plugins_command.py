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

import logging
import shutil
from pprint import pprint

from airflow import plugins_manager

# list to maintain the order of items.
PLUGINS_MANAGER_ATTRIBUTES_TO_DUMP = [
    "plugins",
    "import_errors",
    "operators_modules",
    "sensors_modules",
    "hooks_modules",
    "macros_modules",
    "executors_modules",
    "flask_blueprints",
    "flask_appbuilder_views",
    "flask_appbuilder_menu_links",
    "global_operator_extra_links",
    "operator_extra_links",
    "registered_operator_link_classes",
]
# list to maintain the order of items.
PLUGINS_ATTRIBUTES_TO_DUMP = [
    "operators",
    "sensors",
    "hooks",
    "executors",
    "macros",
    "flask_blueprints",
    "appbuilder_views",
    "appbuilder_menu_items",
    "global_operator_extra_links",
    "operator_extra_links",
]


def _header(text, fillchar):
    terminal_size_size = shutil.get_terminal_size((80, 20))
    print(f" {text} ".center(terminal_size_size.columns, fillchar))


def dump_plugins(args):
    """Dump plugins information"""
    plugins_manager.log.setLevel(logging.DEBUG)

    plugins_manager.ensure_plugins_loaded()
    plugins_manager.integrate_dag_plugins()
    plugins_manager.integrate_executor_plugins()
    plugins_manager.initialize_extra_operators_links_plugins()
    plugins_manager.initialize_web_ui_plugins()

    _header("PLUGINS MANGER:", "#")

    for attr_name in PLUGINS_MANAGER_ATTRIBUTES_TO_DUMP:
        attr_value = getattr(plugins_manager, attr_name)
        print(f"{attr_name} = ", end='')
        pprint(attr_value)
    print()

    _header("PLUGINS:", "#")
    if not plugins_manager.plugins:
        print("No plugins loaded")
    else:
        print(f"Loaded {len(plugins_manager.plugins)} plugins")
        for plugin_no, plugin in enumerate(plugins_manager.plugins, 1):
            _header(f"{plugin_no}. {plugin.name}", "=")
            for attr_name in PLUGINS_ATTRIBUTES_TO_DUMP:
                attr_value = getattr(plugin, attr_name)
                print(f"{attr_name} = ", end='')
                pprint(attr_value)
            print()
