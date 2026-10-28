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

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if not AIRFLOW_V_3_0_PLUS:
    pytest.skip("FAB auth manager is only compatible with Airflow >= 3.0.0", allow_module_level=True)

from pydantic import ValidationError

from airflow.providers.fab.auth_manager.api_fastapi.datamodels.login import LoginResponse


class TestLoginResponse:
    def test_login_response_valid(self):
        token = "test_fab_jwt_access_token_67890"
        response = LoginResponse(access_token=token)
        assert response.access_token == token
        assert response.model_dump() == {"access_token": token}

    def test_login_response_from_dict(self):
        data = {"access_token": "valid_token"}
        response = LoginResponse.model_validate(data)
        assert response.access_token == "valid_token"

    def test_login_response_missing_access_token(self):
        with pytest.raises(ValidationError):
            LoginResponse.model_validate({})
