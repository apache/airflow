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

# This is the class you derive to create a plugin
from airflow.plugins_manager import AirflowPlugin

from flask_admin.base import MenuLink

# Importing base classes that we need to derive
from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.executors.base_executor import BaseExecutor

# Will show up under airflow.hooks.another_test_plugin.AnotherPluginHook
class AnotherPluginHook(BaseHook):
    pass

# Will show up under airflow.operators.another_test_plugin.AnotherPluginOperator
class AnotherPluginOperator(BaseOperator):
    pass

# Will show up under airflow.executors.another_test_plugin.AnotherPluginExecutor
class AnotherPluginExecutor(BaseExecutor):
    pass

# Will show up under airflow.macros.another_test_plugin.plugin_macro
def another_plugin_macro():
    pass

ml = MenuLink(
    category='Another Test Plugin',
    name='Another Test Menu Link',
    url='http://pythonhosted.org/airflow/')

# Defining the plugin class
class AnotherAirflowTestPlugin(AirflowPlugin):
    name = "another_test_plugin"
    operators = [AnotherPluginOperator]
    hooks = [AnotherPluginHook]
    executors = [AnotherPluginExecutor]
    macros = [another_plugin_macro]
    menu_links = [ml]
