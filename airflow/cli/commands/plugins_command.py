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
from typing import Any, List, Optional, Union

from rich.console import Console

from airflow import plugins_manager
from airflow.cli.simple_table import SimpleTable
from airflow.configuration import conf
from airflow.plugins_manager import PluginsDirectorySource

# list to maintain the order of items.
PLUGINS_MANAGER_ATTRIBUTES_TO_DUMP = [
    "plugins",
    "import_errors",
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
    "hooks",
    "executors",
    "macros",
    "flask_blueprints",
    "appbuilder_views",
    "appbuilder_menu_items",
    "global_operator_extra_links",
    "operator_extra_links",
    "source",
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

    console = Console()
    console.print("[bold yellow]SUMMARY:[/bold yellow]")
    console.print(
        f"[bold green]Plugins directory[/bold green]: {conf.get('core', 'plugins_folder')}\n", highlight=False
    )
    console.print(
        f"[bold green]Loaded plugins[/bold green]: {len(plugins_manager.plugins)}\n", highlight=False
    )

    for attr_name in PLUGINS_MANAGER_ATTRIBUTES_TO_DUMP:
        attr_value: Optional[List[Any]] = getattr(plugins_manager, attr_name)
        if not attr_value:
            continue
        table = SimpleTable(title=attr_name.capitalize().replace("_", " "))
        table.add_column(width=100)
        for item in attr_value:  # pylint: disable=not-an-iterable
            table.add_row(
                f"- {_get_name(item)}",
            )
        console.print(table)

    console.print("[bold yellow]DETAILED INFO:[/bold yellow]")
    for plugin in plugins_manager.plugins:
        table = SimpleTable(title=plugin.name)
        for attr_name in PLUGINS_ATTRIBUTES_TO_DUMP:
            value = getattr(plugin, attr_name)
            if not value:
                continue
            table.add_row(attr_name.capitalize().replace("_", " "), _join_plugins_names(value))
        console.print(table)
