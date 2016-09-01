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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import inspect
import logging
import unittest

from flask.blueprints import Blueprint
from flask_admin import BaseView
from flask_admin.menu import MenuLink, MenuView

from airflow.hooks.base_hook import BaseHook
from airflow.models import  BaseOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.www.app import cached_app


class PluginsTest(unittest.TestCase):

    def test_operators(self):
        from airflow.operators.test_plugin import PluginOperator
        assert issubclass(PluginOperator, BaseOperator)

    def test_hooks(self):
        from airflow.hooks.test_plugin import PluginHook
        assert issubclass(PluginHook, BaseHook)

    def test_executors(self):
        from airflow.executors.test_plugin import PluginExecutor
        assert issubclass(PluginExecutor, BaseExecutor)

    def test_macros(self):
        from airflow.macros.test_plugin import plugin_macro
        assert callable(plugin_macro)

    def test_admin_views(self):
        app = cached_app()
        [admin] = app.extensions['admin']
        category = admin._menu_categories['Test Plugin']
        [admin_view] = [v for v in category.get_children()
                        if isinstance(v, MenuView)]
        assert admin_view.name == 'Test View'

    def test_flask_blueprints(self):
        app = cached_app()
        assert isinstance(app.blueprints['test_plugin'], Blueprint)

    def test_menu_links(self):
        app = cached_app()
        [admin] = app.extensions['admin']
        category = admin._menu_categories['Test Plugin']
        [menu_link] = [ml for ml in category.get_children()
                       if isinstance(ml, MenuLink)]
        assert menu_link.name == 'Test Menu Link'
