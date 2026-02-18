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
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.github import GitHubProvider
from pydantic_ai.providers.openai import OpenAIProvider

from airflow.providers.common.ai.exceptions import ModelCreationError
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.sdk import Connection

MODEL_NAME = "github:openai/gpt-5-mini"
API_KEY = "gpt_api_key"


class TestPydanticAIHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        conn = Connection(conn_id="pydantic_ai_default", conn_type=PydanticAIHook.conn_type, password=API_KEY)
        conn_with_extra_fields = Connection(
            conn_id="pydantic_ai_with_extra_fields",
            conn_type=PydanticAIHook.conn_type,
            password=API_KEY,
            extra='{"provider_model": "github:openai/gpt-5-mini"}',
        )

        conn_postgres = Connection(
            conn_id="postgres_default",
            conn_type="postgres",
            password="postgres_password",
            host="postgres_host",
        )
        create_connection_without_db(conn)
        create_connection_without_db(conn_with_extra_fields)
        create_connection_without_db(conn_postgres)

    def test_init(self):
        hook = PydanticAIHook(provider_model=MODEL_NAME)
        assert hook.pydantic_ai_conn_id == "pydantic_ai_default"
        assert hook.provider_model == MODEL_NAME
        assert hook._api_key is None
        assert hook.connection is None

    def test_get_ui_field_behaviour(self):
        behaviour = PydanticAIHook.get_ui_field_behaviour()
        assert "hidden_fields" in behaviour
        assert "relabeling" in behaviour
        assert "placeholders" in behaviour
        assert behaviour["relabeling"]["password"] == "API Key"

    def test_get_conn(self):
        hook = PydanticAIHook()
        conn = hook.get_conn()
        assert conn.password == API_KEY

    def test_get_conn_with_extra_fields(self):
        hook = PydanticAIHook(provider_model=MODEL_NAME, pydantic_ai_conn_id="pydantic_ai_with_extra_fields")
        conn = hook.get_conn()
        assert conn.extra_dejson == {"provider_model": MODEL_NAME}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.BaseHook.get_connection")
    def test_get_conn_multiple_calls(self, mock_get_connection):
        mock_conn = Mock(spec=Connection)
        mock_get_connection.return_value = mock_conn
        hook = PydanticAIHook()
        conn = hook.get_conn()
        assert conn == mock_conn
        assert hook.connection == mock_conn

        hook.get_conn()
        mock_get_connection.assert_called_once()

    def test_get_provider_model_name_from_conn(self):
        hook = PydanticAIHook(pydantic_ai_conn_id="pydantic_ai_with_extra_fields")
        assert hook.provider_model_name_from_conn == MODEL_NAME

    def test_get_api_key_from_conn(self):
        hook = PydanticAIHook()
        assert hook._api_key_from_conn == API_KEY

    @pytest.mark.parametrize(
        "model_settings",
        [
            ({"model_settings": {"max_tokens": 100, "temperature": 0.5}}),
            ({"model_settings": None}),
        ],
        ids=["with_model_settings", "without_model_settings"],
    )
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.BaseHook.get_connection")
    def test_get_model_success(self, mock_get_connection, model_settings):
        mock_conn = Mock(spec=Connection)
        mock_conn.extra_dejson = {"provider_model": MODEL_NAME, **model_settings}
        mock_conn.password = API_KEY
        mock_get_connection.return_value = mock_conn

        hook = PydanticAIHook()
        model = hook.get_model()
        assert isinstance(model, OpenAIChatModel)
        assert isinstance(model._provider, GitHubProvider)
        assert model._settings == model_settings["model_settings"]
        assert model.model_name == MODEL_NAME.split(":", 1)[1]

    def test_get_model_error(self):
        hook = PydanticAIHook(
            pydantic_ai_conn_id="pydantic_ai_with_extra_fields", provider_model="invalid_model"
        )
        with pytest.raises(ModelCreationError):
            hook.get_model()

    def test__provider_factory(self):
        hook = PydanticAIHook()
        provider = hook._provider_factory("openai")
        assert isinstance(provider, OpenAIProvider)
