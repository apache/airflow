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

from unittest import TestCase

import pytest

from airflow.www_rbac import app as application
from tests.compat import mock


class TestPluginsRBAC(TestCase):
    def setup_method(self, method):
        self.app, self.appbuilder = application.create_app(testing=True)

    def test_flaskappbuilder_views(self):
        from tests.plugins.test_plugin import v_appbuilder_package
        appbuilder_class_name = str(v_appbuilder_package['view'].__class__.__name__)
        plugin_views = [view for view in self.appbuilder.baseviews
                        if view.blueprint.name == appbuilder_class_name]

        assert len(plugin_views) == 1

        # view should have a menu item matching category of v_appbuilder_package
        links = [menu_item for menu_item in self.appbuilder.menu.menu
                 if menu_item.name == v_appbuilder_package['category']]

        assert len(links) == 1

        # menu link should also have a link matching the name of the package.
        link = links[0]
        assert link.name == v_appbuilder_package['category']
        assert link.childs[0].name == v_appbuilder_package['name']

    def test_flaskappbuilder_menu_links(self):
        from tests.plugins.test_plugin import appbuilder_mitem

        # menu item should exist matching appbuilder_mitem
        links = [menu_item for menu_item in self.appbuilder.menu.menu
                 if menu_item.name == appbuilder_mitem['category']]

        assert len(links) == 1

        # menu link should also have a link matching the name of the package.
        link = links[0]
        assert link.name == appbuilder_mitem['category']
        assert link.childs[0].name == appbuilder_mitem['name']

    def test_app_blueprints(self):
        from tests.plugins.test_plugin import bp

        # Blueprint should be present in the app
        assert 'test_plugin' in self.app.blueprints
        assert self.app.blueprints['test_plugin'].name == bp.name

    @pytest.mark.quarantined
    def test_entrypoint_plugin_errors_dont_raise_exceptions(self):
        """
        Test that Airflow does not raise an Error if there is any Exception because of the
        Plugin.
        """
        from airflow.plugins_manager import load_entrypoint_plugins, entry_points_with_dist

        mock_dist = mock.Mock()

        mock_entrypoint = mock.Mock()
        mock_entrypoint.name = 'test-entrypoint'
        mock_entrypoint.group = 'airflow.plugins'
        mock_entrypoint.module = 'test.plugins.test_plugins_manager'
        mock_entrypoint.load.side_effect = ImportError('my_fake_module not found')
        mock_dist.entry_points = [mock_entrypoint]

        with mock.patch(
            'importlib_metadata.distributions', return_value=[mock_dist]
        ), self.assertLogs("airflow.plugins_manager", level="ERROR") as log_output:
            load_entrypoint_plugins(entry_points_with_dist('airflow.plugins'), [])

            received_logs = log_output.output[0]
            # Assert Traceback is shown too
            assert "Traceback (most recent call last):" in received_logs
            assert "my_fake_module not found" in received_logs
            assert "Failed to import plugin test-entrypoint" in received_logs
