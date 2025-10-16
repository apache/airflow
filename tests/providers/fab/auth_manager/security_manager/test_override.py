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
from unittest.mock import Mock

from tests_common.test_utils.compat import ignore_provider_compatibility_error

with ignore_provider_compatibility_error("2.9.0+", __file__):
    from airflow.providers.fab.auth_manager.security_manager.override import FabAirflowSecurityManagerOverride


class EmptySecurityManager(FabAirflowSecurityManagerOverride):
    # noinspection PyMissingConstructor
    # super() not called on purpose to avoid the whole chain of init calls
    def __init__(self):
        pass


class TestFabAirflowSecurityManagerOverride:
    def test_load_user(self):
        sm = EmptySecurityManager()
        sm.get_user_by_id = Mock()

        sm.load_user("123")

        sm.get_user_by_id.assert_called_once_with(123)

    @mock.patch("airflow.providers.fab.auth_manager.security_manager.override.g", spec={})
    def test_load_user_jwt(self, mock_g):
        sm = EmptySecurityManager()
        mock_user = Mock()
        sm.load_user = Mock(return_value=mock_user)

        actual_user = sm.load_user_jwt(None, {"sub": "test_identity"})

        sm.load_user.assert_called_once_with("test_identity")
        assert actual_user is mock_user
        assert mock_g.user is mock_user
