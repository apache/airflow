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
import unittest
from unittest import mock

from airflow.hooks.base_hook import BaseHook
from airflow.plugins_manager import AirflowPlugin
from airflow.www import app as application
from tests.test_utils.mock_plugins import mock_plugin_manager


class TestPluginsRBAC(unittest.TestCase):
    def setUp(self):
        self.app = application.create_app(testing=True)
        self.appbuilder = self.app.appbuilder  # pylint: disable=no-member

    def test_flaskappbuilder_views(self):
        from tests.plugins.test_plugin import v_appbuilder_package

        appbuilder_class_name = str(v_appbuilder_package['view'].__class__.__name__)
        plugin_views = [
            view for view in self.appbuilder.baseviews if view.blueprint.name == appbuilder_class_name
        ]

        self.assertTrue(len(plugin_views) == 1)

        # view should have a menu item matching category of v_appbuilder_package
        links = [
            menu_item
            for menu_item in self.appbuilder.menu.menu
            if menu_item.name == v_appbuilder_package['category']
        ]

        self.assertTrue(len(links) == 1)

        # menu link should also have a link matching the name of the package.
        link = links[0]
        self.assertEqual(link.name, v_appbuilder_package['category'])
        self.assertEqual(link.childs[0].name, v_appbuilder_package['name'])

    def test_flaskappbuilder_menu_links(self):
        from tests.plugins.test_plugin import appbuilder_mitem

        # menu item should exist matching appbuilder_mitem
        links = [
            menu_item
            for menu_item in self.appbuilder.menu.menu
            if menu_item.name == appbuilder_mitem['category']
        ]

        self.assertTrue(len(links) == 1)

        # menu link should also have a link matching the name of the package.
        link = links[0]
        self.assertEqual(link.name, appbuilder_mitem['category'])
        self.assertEqual(link.childs[0].name, appbuilder_mitem['name'])

    def test_app_blueprints(self):
        from tests.plugins.test_plugin import bp

        # Blueprint should be present in the app
        self.assertTrue('test_plugin' in self.app.blueprints)
        self.assertEqual(self.app.blueprints['test_plugin'].name, bp.name)

    @mock.patch('airflow.plugins_manager.pkg_resources.iter_entry_points')
    def test_entrypoint_plugin_errors_dont_raise_exceptions(self, mock_ep_plugins):
        """
        Test that Airflow does not raise an Error if there is any Exception because of the
        Plugin.
        """
        from airflow.plugins_manager import import_errors, load_entrypoint_plugins

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = 'test-entrypoint'
        mock_entrypoint.module_name = 'test.plugins.test_plugins_manager'
        mock_entrypoint.load.side_effect = Exception('Version Conflict')
        mock_ep_plugins.return_value = [mock_entrypoint]

        with self.assertLogs("airflow.plugins_manager", level="ERROR") as log_output:
            load_entrypoint_plugins()

            received_logs = log_output.output[0]
            # Assert Traceback is shown too
            assert "Traceback (most recent call last):" in received_logs
            assert "Version Conflict" in received_logs
            assert "Failed to import plugin test-entrypoint" in received_logs
            assert ("test.plugins.test_plugins_manager", "Version Conflict") in import_errors.items()


class TestPluginsManager(unittest.TestCase):
    def test_should_load_plugins_from_property(self):
        class AirflowTestPropertyPlugin(AirflowPlugin):
            name = "test_property_plugin"

            @property
            def operators(self):
                from airflow.models.baseoperator import BaseOperator

                class PluginPropertyOperator(BaseOperator):
                    pass

                return [PluginPropertyOperator]

            class TestNonPropertyHook(BaseHook):
                pass

            hooks = [TestNonPropertyHook]

        with mock_plugin_manager(plugins=[AirflowTestPropertyPlugin()]):
            from airflow import plugins_manager

            plugins_manager.integrate_dag_plugins()

            self.assertIn('AirflowTestPropertyPlugin', str(plugins_manager.plugins))
            self.assertIn('PluginPropertyOperator', str(plugins_manager.operators_modules[0].__dict__))
            self.assertIn("TestNonPropertyHook", str(plugins_manager.hooks_modules[0].__dict__))

    def test_should_warning_about_incompatible_plugins(self):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]

        with mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]):
            from airflow import plugins_manager

            # assert not logs
            with self.assertLogs(plugins_manager.log) as cm:
                plugins_manager.initialize_web_ui_plugins()

        self.assertEqual(
            cm.output,
            [
                'WARNING:airflow.plugins_manager:Plugin \'test_admin_views_plugin\' may not be '
                'compatible with the current Airflow version. Please contact the author of '
                'the plugin.',
                'WARNING:airflow.plugins_manager:Plugin \'test_menu_links_plugin\' may not be '
                'compatible with the current Airflow version. Please contact the author of '
                'the plugin.',
            ],
        )

    def test_should_not_warning_about_fab_plugins(self):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            appbuilder_menu_items = [mock.MagicMock()]

        with mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]):
            from airflow import plugins_manager

            # assert not logs
            with self.assertRaises(AssertionError), self.assertLogs(plugins_manager.log):
                plugins_manager.initialize_web_ui_plugins()

    def test_should_not_warning_about_fab_and_flask_admin_plugins(self):
        class AirflowAdminViewsPlugin(AirflowPlugin):
            name = "test_admin_views_plugin"

            admin_views = [mock.MagicMock()]
            appbuilder_views = [mock.MagicMock()]

        class AirflowAdminMenuLinksPlugin(AirflowPlugin):
            name = "test_menu_links_plugin"

            menu_links = [mock.MagicMock()]
            appbuilder_menu_items = [mock.MagicMock()]

        with mock_plugin_manager(plugins=[AirflowAdminViewsPlugin(), AirflowAdminMenuLinksPlugin()]):
            from airflow import plugins_manager

            # assert not logs
            with self.assertRaises(AssertionError), self.assertLogs(plugins_manager.log):
                plugins_manager.initialize_web_ui_plugins()


class TestPluginsDirectorySource(unittest.TestCase):
    def test_should_return_correct_path_name(self):
        from airflow import plugins_manager

        source = plugins_manager.PluginsDirectorySource(__file__)
        self.assertEqual("test_plugins_manager.py", source.path)
        self.assertEqual("$PLUGINS_FOLDER/test_plugins_manager.py", str(source))
        self.assertEqual("<em>$PLUGINS_FOLDER/</em>test_plugins_manager.py", source.__html__())


class TestEntryPointSource(unittest.TestCase):
    @mock.patch('airflow.plugins_manager.pkg_resources.iter_entry_points')
    def test_should_return_correct_source_details(self, mock_ep_plugins):
        from airflow import plugins_manager

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = 'test-entrypoint-plugin'
        mock_entrypoint.module_name = 'module_name_plugin'
        mock_entrypoint.dist = 'test-entrypoint-plugin==1.0.0'
        mock_ep_plugins.return_value = [mock_entrypoint]

        plugins_manager.load_entrypoint_plugins()

        source = plugins_manager.EntryPointSource(mock_entrypoint)
        self.assertEqual(str(mock_entrypoint), source.entrypoint)
        self.assertEqual("test-entrypoint-plugin==1.0.0: " + str(mock_entrypoint), str(source))
        self.assertEqual("<em>test-entrypoint-plugin==1.0.0:</em> " + str(mock_entrypoint), source.__html__())
