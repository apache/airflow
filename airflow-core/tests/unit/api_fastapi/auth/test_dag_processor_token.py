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

import pytest

from airflow.api_fastapi.auth import dag_processor_token

from tests_common.test_utils.config import conf_vars


class TestMintDagProcessorToken:
    @mock.patch.object(dag_processor_token, "get_signing_args", return_value={"secret_key": "s"})
    @mock.patch.object(dag_processor_token, "JWTGenerator")
    def test_carries_both_audiences_and_subject(self, mock_generator, _signing_args):
        mock_generator.return_value.generate.return_value = "minted-token"
        with conf_vars(
            {
                ("execution_api", "jwt_audience"): "exec-aud",
                ("dag_processor", "jwt_audience"): "dag-proc-aud",
                ("dag_processor", "jwt_expiration_time"): "123",
            }
        ):
            token = dag_processor_token.mint_dag_processor_token()

        assert token == "minted-token"
        # Both audiences, since the processor presents this one token to both APIs.
        assert mock_generator.call_args.kwargs["audience"] == ["exec-aud", "dag-proc-aud"]
        assert mock_generator.call_args.kwargs["valid_for"] == 123
        mock_generator.return_value.generate.assert_called_once_with(
            {"sub": dag_processor_token.DAG_PROCESSOR_TOKEN_SUBJECT}
        )

    @mock.patch.object(dag_processor_token, "get_signing_args", return_value={"secret_key": "s"})
    @mock.patch.object(dag_processor_token, "JWTGenerator")
    def test_explicit_valid_for_overrides_config(self, mock_generator, _signing_args):
        with conf_vars(
            {
                ("execution_api", "jwt_audience"): "exec-aud",
                ("dag_processor", "jwt_audience"): "dag-proc-aud",
            }
        ):
            dag_processor_token.mint_dag_processor_token(valid_for=42)
        assert mock_generator.call_args.kwargs["valid_for"] == 42


class TestProvisionDagProcessorTokenFile:
    @mock.patch.object(dag_processor_token, "mint_dag_processor_token", return_value="minted")
    def test_writes_to_explicit_path_and_creates_parents(self, _mint, tmp_path):
        target = tmp_path / "nested" / "dag-processor-token"

        returned = dag_processor_token.provision_dag_processor_token_file(target)

        assert returned == str(target)
        assert target.read_text() == "minted"
        # Atomic write leaves no temp file behind.
        assert not (target.parent / "dag-processor-token.tmp").exists()

    @mock.patch.object(dag_processor_token, "mint_dag_processor_token", return_value="minted")
    def test_defaults_to_configured_api_token_path(self, _mint, tmp_path):
        target = tmp_path / "token"
        with conf_vars({("dag_processor", "api_token_path"): str(target)}):
            dag_processor_token.provision_dag_processor_token_file()
        assert target.read_text() == "minted"

    def test_raises_when_no_path_configured(self):
        with conf_vars({("dag_processor", "api_token_path"): ""}):
            with pytest.raises(ValueError, match="api_token_path"):
                dag_processor_token.provision_dag_processor_token_file(None)
