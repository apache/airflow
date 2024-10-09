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
from fastapi import HTTPException
from jwt import InvalidTokenError

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.core_api.security import get_user, requires_access_dag
from airflow.auth.managers.models.resource_details import DagAccessEntity
from airflow.auth.managers.simple.user import SimpleAuthManagerUser

from tests_common.test_utils.config import conf_vars


class TestFastApiSecurity:
    @classmethod
    def setup_class(cls):
        with conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            create_app()

    @patch("airflow.api_fastapi.core_api.security.get_signer")
    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_get_user(self, mock_get_auth_manager, mock_get_signer):
        token_str = "test-token"
        user_dict = {"user": "XXXXXXXXX"}
        user = SimpleAuthManagerUser(username="username", role="admin")

        auth_manager = Mock()
        auth_manager.deserialize_user.return_value = user
        mock_get_auth_manager.return_value = auth_manager

        signer = Mock()
        signer.verify_token.return_value = user_dict
        mock_get_signer.return_value = signer

        result = get_user(token_str)

        signer.verify_token.assert_called_once_with(token_str)
        auth_manager.deserialize_user.assert_called_once_with(user_dict)
        assert result == user

    @patch("airflow.api_fastapi.core_api.security.get_signer")
    def test_get_user_unsuccessful(self, mock_get_signer):
        token_str = "test-token"

        signer = Mock()
        signer.verify_token.side_effect = InvalidTokenError()
        mock_get_signer.return_value = signer

        with pytest.raises(HTTPException, match="Forbidden"):
            get_user(token_str)

        signer.verify_token.assert_called_once_with(token_str)

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_dag_authorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value = auth_manager

        requires_access_dag("GET", DagAccessEntity.CODE)("dag-id", Mock())

        auth_manager.is_authorized_dag.assert_called_once()

    @patch("airflow.api_fastapi.core_api.security.get_auth_manager")
    def test_requires_access_dag_unauthorized(self, mock_get_auth_manager):
        auth_manager = Mock()
        auth_manager.is_authorized_dag.return_value = False
        mock_get_auth_manager.return_value = auth_manager

        with pytest.raises(HTTPException, match="Forbidden"):
            requires_access_dag("GET", DagAccessEntity.CODE)("dag-id", Mock())

        auth_manager.is_authorized_dag.assert_called_once()
