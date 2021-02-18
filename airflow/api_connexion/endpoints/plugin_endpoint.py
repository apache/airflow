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
from airflow import plugins_manager
from airflow.api_connexion import security
from airflow.api_connexion.exceptions import NotFound
from airflow.api_connexion.schemas.plugin_schema import PluginCollection, plugin_collection_schema
from airflow.cli.commands.plugins_command import PLUGINS_ATTRIBUTES_TO_DUMP
from airflow.security import permissions


@security.requires_access(
    [
        (permissions.ACTION_CAN_READ, permissions.RESOURCE_PLUGIN),
        (permissions.ACTION_CAN_ACCESS_MENU, permissions.RESOURCE_PLUGIN),
    ]
)
def get_plugins():
    """Get plugins endpoint"""
    plugins_manager.ensure_plugins_loaded()
    plugins_manager.integrate_macros_plugins()
    plugins_manager.integrate_executor_plugins()
    plugins_manager.initialize_extra_operators_links_plugins()
    plugins_manager.initialize_web_ui_plugins()
    plugins = []
    for plugin in plugins_manager.plugins:
        info = {"name": plugin.name}
        info.update({n: getattr(plugin, n) for n in PLUGINS_ATTRIBUTES_TO_DUMP})
        plugins.append(info)
    if plugins:
        return plugin_collection_schema.dump(PluginCollection(plugins=plugins, total_entries=len(plugins)))
    raise NotFound(detail="Plugin not found")
