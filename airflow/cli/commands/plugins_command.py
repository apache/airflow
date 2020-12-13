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
import inspect
from typing import Any, Dict, List, Union

from airflow import plugins_manager
from airflow.cli.simple_table import AirflowConsole
from airflow.plugins_manager import PluginsDirectorySource
from airflow.utils.cli import suppress_logs_and_warning

# list to maintain the order of items.
PLUGINS_ATTRIBUTES_TO_DUMP = [
    "source",
    "hooks",
    "executors",
    "macros",
    "flask_blueprints",
    "appbuilder_views",
    "appbuilder_menu_items",
    "global_operator_extra_links",
    "operator_extra_links",
]


def _get_name(class_like_object) -> str:
    if isinstance(class_like_object, (str, PluginsDirectorySource)):
        return str(class_like_object)
    if inspect.isclass(class_like_object):
        return class_like_object.__name__
    return class_like_object.__class__.__name__


def _join_plugins_names(value: Union[List[Any], Any]) -> str:
    value = value if isinstance(value, list) else [value]
    return ",".join(_get_name(v) for v in value)


@suppress_logs_and_warning()
def dump_plugins(args):
    """Dump plugins information"""
    plugins_manager.ensure_plugins_loaded()
    plugins_manager.integrate_macros_plugins()
    plugins_manager.integrate_executor_plugins()
    plugins_manager.initialize_extra_operators_links_plugins()
    plugins_manager.initialize_web_ui_plugins()
    if not plugins_manager.plugins:
        print("No plugins loaded")
        return

    plugins_info: List[Dict[str, str]] = []
    for plugin in plugins_manager.plugins:
        info = {"name": plugin.name}
        info.update({n: getattr(plugin, n) for n in PLUGINS_ATTRIBUTES_TO_DUMP})
        plugins_info.append(info)

    # Remove empty info
    if args.output == "table":  # pylint: disable=too-many-nested-blocks
        # We can do plugins_info[0] as the element it will exist as there's
        # at least one plugin at this point
        for col in list(plugins_info[0]):
            if all(not bool(p[col]) for p in plugins_info):
                for plugin in plugins_info:
                    del plugin[col]

    AirflowConsole().print_as(plugins_info, output=args.output)
