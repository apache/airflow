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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import unittest
from mock import MagicMock, PropertyMock

from flask.blueprints import Blueprint
from flask_admin.menu import MenuLink, MenuView

from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.plugins_manager import load_entrypoint_plugins, is_valid_plugin
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.executors.base_executor import BaseExecutor
from airflow.www.app import create_app

from tests.plugins.test_plugin import MockPluginA, MockPluginB, MockPluginC


class PluginsTest(unittest.TestCase):

    def test_operators(self):
        from airflow.operators.test_plugin import PluginOperator
        self.assertTrue(issubclass(PluginOperator, BaseOperator))

    def test_sensors(self):
        from airflow.sensors.test_plugin import PluginSensorOperator
        self.assertTrue(issubclass(PluginSensorOperator, BaseSensorOperator))

    def test_hooks(self):
        from airflow.hooks.test_plugin import PluginHook
        self.assertTrue(issubclass(PluginHook, BaseHook))

    def test_executors(self):
        from airflow.executors.test_plugin import PluginExecutor
        self.assertTrue(issubclass(PluginExecutor, BaseExecutor))

        from airflow.executors import get_default_executor
        self.assertTrue(issubclass(type(get_default_executor()), BaseExecutor))

        # test plugin executor import based on a name string, (like defined in airflow.cfg)
        # this is not identical to the first assertion!
        from airflow.executors import _get_executor
        self.assertTrue(issubclass(type(_get_executor('test_plugin.PluginExecutor')), BaseExecutor))

    def test_macros(self):
        from airflow.macros.test_plugin import plugin_macro
        self.assertTrue(callable(plugin_macro))

    def test_admin_views(self):
        app = create_app(testing=True)
        [admin] = app.extensions['admin']
        category = admin._menu_categories['Test Plugin']
        [admin_view] = [v for v in category.get_children()
                        if isinstance(v, MenuView)]
        self.assertEqual('Test View', admin_view.name)

    def test_flask_blueprints(self):
        app = create_app(testing=True)
        self.assertIsInstance(app.blueprints['test_plugin'], Blueprint)

    def test_menu_links(self):
        app = create_app(testing=True)
        [admin] = app.extensions['admin']
        category = admin._menu_categories['Test Plugin']
        [menu_link] = [ml for ml in category.get_children()
                       if isinstance(ml, MenuLink)]
        self.assertEqual('Test Menu Link', menu_link.name)


class PluginsTestEntrypointLoad(unittest.TestCase):

    def setUp(self):
        self.expected = [MockPluginA, MockPluginB, MockPluginC]
        self.entrypoints = [
            self._build_mock(plugin_obj)
            for plugin_obj in self.expected
        ]

    def _build_mock(self, plugin_obj):
        m = MagicMock(**{
            'load.return_value': plugin_obj
        })
        type(m).name = PropertyMock(return_value='plugin-' + plugin_obj.name)
        return m

    def test_load_entrypoint_plugins(self):
        self.assertListEqual(
            load_entrypoint_plugins(self.entrypoints, []),
            self.expected
        )

    def test_failed_load_entrpoint_plugins(self):
        self.assertListEqual(
            load_entrypoint_plugins(
                self.entrypoints[:2] + [self._build_mock(
                    MagicMock(name='other')
                )], []),
            self.expected[:2]
        )


class PluginsTestValidator(unittest.TestCase):
    def setUp(self):
        self.existing = [MockPluginA, MockPluginB]

    def test_valid_plugin(self):
        c = MockPluginC
        self.assertTrue(
            is_valid_plugin(
                c, self.existing
            ))

    def test_invalid_plugin(self):
        c = MockPluginC
        self.assertFalse(
            is_valid_plugin(
                c, self.existing + [c]
            ))
