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

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from airflow.providers.fab.auth_manager.api_fastapi.security import requires_fab_custom_view


class TestSecurityDependency:
    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    def test_requires_fab_custom_view_allows_when_authorized(self, get_auth_manager):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = True
        get_auth_manager.return_value = mgr

        check = requires_fab_custom_view(method="POST", resource_name="Role")
        user = object()

        assert check(user=user) is None
        mgr.is_authorized_custom_view.assert_called_once_with(method="POST", resource_name="Role", user=user)

    @patch("airflow.providers.fab.auth_manager.api_fastapi.security.get_auth_manager")
    def test_requires_fab_custom_view_raises_403_when_unauthorized(self, get_auth_manager):
        mgr = MagicMock()
        mgr.is_authorized_custom_view.return_value = False
        get_auth_manager.return_value = mgr

        check = requires_fab_custom_view(method="DELETE", resource_name="Role")
        with pytest.raises(HTTPException) as ex:
            check(user=object())
        assert ex.value.status_code == 403
