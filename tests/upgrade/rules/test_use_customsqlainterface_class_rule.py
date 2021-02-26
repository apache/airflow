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

from unittest import TestCase

from flask_appbuilder.models.sqla.interface import SQLAInterface

from airflow.models import User
from airflow.upgrade.rules.use_customsqlainterface_class_rule import UseCustomSQLAInterfaceClassRule
from airflow.www_rbac.utils import CustomSQLAInterface
from tests.compat import patch, MagicMock

mock_appbuilder_sqla_interface = MagicMock(datamodel=SQLAInterface(User))

mock_appbuilder_custom_sqla_interface = MagicMock(datamodel=CustomSQLAInterface(User))

test_plugins_sqla_interface = [{
    "name": "Test View",
    "category": "Test Plugin",
    "endpoint": "plugin",
    "view": mock_appbuilder_sqla_interface
}, {
    "name": "Test View",
    "category": "Test Plugin",
    "endpoint": "plugin",
    "view": mock_appbuilder_sqla_interface
}]

test_plugins_custom_sqla_interface = [{
    "name": "Test View",
    "category": "Test Plugin",
    "endpoint": "plugin",
    "view": mock_appbuilder_custom_sqla_interface
}, {
    "name": "Test View",
    "category": "Test Plugin",
    "endpoint": "plugin",
    "view": mock_appbuilder_custom_sqla_interface
}]


class TestUseCustomSQLAInterfaceClassRule(TestCase):

    @patch('airflow.plugins_manager.flask_appbuilder_views', test_plugins_sqla_interface)
    def test_invalid_check(self):
        rule = UseCustomSQLAInterfaceClassRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        msg = (
            "Deprecation Warning: The following views: ['Test View', 'Test View'] have "
            "data models instantiated from the SQLAInterface class.\n"
            "See: "
            "https://github.com/apache/airflow/blob/master/"
            "UPDATING.md#use-customsqlainterface-instead-of-sqlainterface-for-custom-data-models"
        )

        assert msg == rule.check()

    @patch('airflow.plugins_manager.flask_appbuilder_views', test_plugins_custom_sqla_interface)
    def test_valid_check(self):
        rule = UseCustomSQLAInterfaceClassRule()

        assert isinstance(rule.title, str)
        assert isinstance(rule.description, str)

        assert rule.check() is None
