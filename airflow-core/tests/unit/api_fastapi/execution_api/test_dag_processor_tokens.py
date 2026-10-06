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

import stat
from unittest import mock

import pytest
import uuid6

from airflow.api_fastapi.execution_api.app import _jwt_validator
from airflow.api_fastapi.execution_api.dag_processor_tokens import (
    generate_dag_processor_session_token,
    write_token_file,
)
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims

from tests_common.test_utils.config import conf_vars


@conf_vars({("api_auth", "jwt_secret"): "provisioning-test-secret"})
def test_generated_token_is_a_valid_dag_processor_session_token():
    session_id = uuid6.uuid7()

    token = generate_dag_processor_session_token(
        session_id=session_id, bundle_names={"b", "a"}, valid_for=120
    )

    claims = _jwt_validator().validated_claims(token)
    assert claims["sub"] == str(session_id)
    assert claims["dag_bundles"] == ["a", "b"]
    assert claims["exp"] - claims["iat"] == 120
    parsed = TIClaims(**claims)
    assert (parsed.scope, parsed.dag_bundles) == ("dag_processor_session", frozenset({"a", "b"}))


class TestWriteTokenFile:
    def test_replaces_the_file_readable_only_by_its_owner(self, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")

        write_token_file(token_file, "new")

        assert token_file.read_text() == "new"
        assert stat.S_IMODE(token_file.stat().st_mode) == 0o600
        assert [path.name for path in tmp_path.iterdir()] == ["token"]

    @mock.patch(
        "airflow.api_fastapi.execution_api.dag_processor_tokens.os.replace",
        autospec=True,
        side_effect=OSError("disk full"),
    )
    def test_failure_keeps_the_previous_token(self, _, tmp_path):
        token_file = tmp_path / "token"
        token_file.write_text("old")

        with pytest.raises(OSError, match="disk full"):
            write_token_file(token_file, "new")

        assert token_file.read_text() == "old"
        assert [path.name for path in tmp_path.iterdir()] == ["token"]
