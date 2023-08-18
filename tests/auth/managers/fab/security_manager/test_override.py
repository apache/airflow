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

from unittest import mock
from unittest.mock import MagicMock, Mock

import pytest

from airflow.auth.managers.fab.models import User
from airflow.auth.managers.fab.security_manager.override import FabAirflowSecurityManagerOverride

appbuilder = Mock()
actionmodelview = Mock()
authdbview = Mock()
authldapview = Mock()
authoauthview = Mock()
authoidview = Mock()
authremoteuserview = Mock()
permissionmodelview = Mock()
registeruser_view = Mock()
registeruserdbview = Mock()
registeruseroauthview = Mock()
registerusermodelview = Mock()
registeruseroidview = Mock()
resetmypasswordview = Mock()
resetpasswordview = Mock()
rolemodelview = Mock()
user_model = User
userinfoeditview = Mock()
userdbmodelview = Mock()
userldapmodelview = Mock()
useroauthmodelview = Mock()
useroidmodelview = Mock()
userremoteusermodelview = Mock()
userstatschartview = Mock()


@pytest.fixture
def security_manager_override():
    class SubSecurityManager:
        def __init__(self, **kwargs):
            pass

    class EmptySecurityManager(FabAirflowSecurityManagerOverride, SubSecurityManager):
        def __init__(self, appbuilder):
            super().__init__(
                appbuilder=appbuilder,
                actionmodelview=actionmodelview,
                authdbview=authdbview,
                authldapview=authldapview,
                authoauthview=authoauthview,
                authoidview=authoidview,
                authremoteuserview=authremoteuserview,
                permissionmodelview=permissionmodelview,
                registeruser_view=registeruser_view,
                registeruserdbview=registeruserdbview,
                registeruseroauthview=registeruseroauthview,
                registerusermodelview=registerusermodelview,
                registeruseroidview=registeruseroidview,
                resetmypasswordview=resetmypasswordview,
                resetpasswordview=resetpasswordview,
                rolemodelview=rolemodelview,
                user_model=user_model,
                userinfoeditview=userinfoeditview,
                userdbmodelview=userdbmodelview,
                userldapmodelview=userldapmodelview,
                useroauthmodelview=useroauthmodelview,
                useroidmodelview=useroidmodelview,
                userremoteusermodelview=userremoteusermodelview,
                userstatschartview=userstatschartview,
            )

    with mock.patch(
        "airflow.auth.managers.fab.security_manager.override.LoginManager"
    ) as mock_login_manager, mock.patch(
        "airflow.auth.managers.fab.security_manager.override.JWTManager"
    ) as mock_jwt_manager, mock.patch.object(
        FabAirflowSecurityManagerOverride, "create_db"
    ):
        mock_login_manager_instance = Mock()
        mock_login_manager.return_value = mock_login_manager_instance
        mock_jwt_manager_instance = Mock()
        mock_jwt_manager.return_value = mock_jwt_manager_instance

        appbuilder.app.config = MagicMock()

        security_manager_override = EmptySecurityManager(appbuilder)

        mock_login_manager.assert_called_once_with(appbuilder.app)
        mock_login_manager_instance.user_loader.assert_called_once_with(security_manager_override.load_user)
        mock_jwt_manager_instance.init_app.assert_called_once_with(appbuilder.app)
        mock_jwt_manager_instance.user_lookup_loader.assert_called_once_with(
            security_manager_override.load_user_jwt
        )

    return security_manager_override


class TestFabAirflowSecurityManagerOverride:
    def test_load_user(self, security_manager_override):
        mock_get_user_by_id = Mock()
        security_manager_override.get_user_by_id = mock_get_user_by_id
        security_manager_override.load_user("123")
        mock_get_user_by_id.assert_called_once_with(123)

    @mock.patch("airflow.auth.managers.fab.security_manager.override.g", spec={})
    def test_load_user_jwt(self, mock_g, security_manager_override):
        mock_user = Mock()
        mock_load_user = Mock(return_value=mock_user)
        security_manager_override.load_user = mock_load_user
        actual_user = security_manager_override.load_user_jwt(None, {"sub": "test_identity"})
        mock_load_user.assert_called_once_with("test_identity")
        assert actual_user is mock_user
        assert mock_g.user is mock_user
