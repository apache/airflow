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

from airflow.providers.amazon.aws.hooks.bedrock import (
    BedrockAgentHook,
    BedrockAgentRuntimeHook,
    BedrockHook,
    BedrockRuntimeHook,
)


class TestBedrockHooks:
    @pytest.mark.parametrize(
        ("test_hook", "service_name"),
        [
            pytest.param(BedrockHook(), "bedrock", id="bedrock"),
            pytest.param(BedrockRuntimeHook(), "bedrock-runtime", id="bedrock-runtime"),
            pytest.param(BedrockAgentHook(), "bedrock-agent", id="bedrock-agent"),
            pytest.param(BedrockAgentRuntimeHook(), "bedrock-agent-runtime", id="bedrock-agent-runtime"),
        ],
    )
    def test_bedrock_hooks(self, test_hook, service_name):
        assert test_hook.conn is not None
        assert test_hook.conn.meta.service_model.service_name == service_name


class TestBedrockHookGetGuardrailIdByName:
    @mock.patch.object(BedrockHook, "conn", new_callable=mock.PropertyMock)
    def test_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"guardrails": [{"name": "my-guardrail", "id": "abc123"}]}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = BedrockHook()
        assert hook.get_guardrail_id_by_name("my-guardrail") == "abc123"

    @mock.patch.object(BedrockHook, "conn", new_callable=mock.PropertyMock)
    def test_not_found(self, mock_conn):
        mock_client = mock.MagicMock()
        mock_paginator = mock.MagicMock()
        mock_paginator.paginate.return_value = [{"guardrails": []}]
        mock_client.get_paginator.return_value = mock_paginator
        mock_conn.return_value = mock_client

        hook = BedrockHook()
        assert hook.get_guardrail_id_by_name("nonexistent") is None
