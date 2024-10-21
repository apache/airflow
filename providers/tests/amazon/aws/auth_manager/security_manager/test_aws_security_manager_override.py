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
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest
from flask import Flask

from tests_common.test_utils.compat import ignore_provider_compatibility_error

python3_saml = pytest.importorskip("python3-saml")

with ignore_provider_compatibility_error("2.8.0", __file__):
    from airflow.providers.amazon.aws.auth_manager.security_manager.aws_security_manager_override import (
        AwsSecurityManagerOverride,
    )
from airflow.www.extensions.init_appbuilder import init_appbuilder

pytestmark = pytest.mark.skip_if_database_isolation_mode


@pytest.fixture
def appbuilder():
    flask_app = Flask(__name__)
    return init_appbuilder(flask_app)


@pytest.fixture
def override(appbuilder):
    return AwsSecurityManagerOverride(appbuilder)


@pytest.mark.db_test
class TestAwsSecurityManagerOverride:
    @patch(
        "airflow.providers.amazon.aws.auth_manager.views.auth.conf.get_mandatory_value", return_value="test"
    )
    def test_register_views(self, mock_get_mandatory_value, override, appbuilder):
        from airflow.providers.amazon.aws.auth_manager.views.auth import AwsAuthManagerAuthenticationViews

        with patch.object(AwsAuthManagerAuthenticationViews, "idp_data"):
            appbuilder.add_view_no_menu = Mock()
            override.register_views()
            appbuilder.add_view_no_menu.assert_called_once()
            assert isinstance(
                appbuilder.add_view_no_menu.call_args.args[0], AwsAuthManagerAuthenticationViews
            )
