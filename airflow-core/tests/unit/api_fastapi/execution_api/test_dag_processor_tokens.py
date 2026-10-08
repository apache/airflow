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

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
import uuid6

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.app import _jwt_validator, create_jwt_generator
from airflow.api_fastapi.execution_api.dag_processor_tokens import (
    ExpiredDagProcessorToken,
    generate_dag_processor_session_token,
    generate_dag_processor_token,
)
from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorSessionClaims

from tests_common.test_utils.config import conf_vars


@conf_vars({("api_auth", "jwt_secret"): "provisioning-test-secret"})
def test_generated_token_is_a_valid_dag_processor_session_token():
    session_id = uuid6.uuid7()

    token = generate_dag_processor_session_token(
        create_jwt_generator(), session_id=session_id, bundle_names={"b", "a"}, valid_for=120
    )

    claims = _jwt_validator().validated_claims(token)
    assert claims["sub"] == str(session_id)
    assert claims["dag_bundles"] == ["a", "b"]
    assert claims["exp"] - claims["iat"] == 120
    parsed = DagProcessorSessionClaims(**claims)
    assert (parsed.scope, parsed.dag_bundles) == ("dag_processor_session", frozenset({"a", "b"}))


@pytest.mark.parametrize("remaining", [0, -1], ids=["expires-now", "already-expired"])
def test_expired_session_is_not_signed(time_machine, remaining):
    now = datetime(2026, 10, 5, tzinfo=UTC)
    time_machine.move_to(now, tick=False)
    generator = MagicMock(spec=JWTGenerator)

    with pytest.raises(ExpiredDagProcessorToken, match="Session credential has expired"):
        generate_dag_processor_token(
            generator,
            session_id=uuid6.uuid7(),
            job_id=1,
            bundle_names={"bundle"},
            session_expiry=now.timestamp() + remaining,
        )

    generator.generate.assert_not_called()
