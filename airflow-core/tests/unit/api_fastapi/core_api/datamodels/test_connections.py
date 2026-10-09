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

import json

import pytest
from pydantic import ValidationError

from airflow.api_fastapi.core_api.datamodels.connections import ConnectionResponse


def _payload(**overrides):
    base = {
        "conn_id": "conn_1",
        "conn_type": "generic",
        "description": None,
        "host": None,
        "login": None,
        "schema": None,
        "port": None,
        "password": None,
        "extra": None,
        "team_name": None,
    }
    return {**base, **overrides}


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param("bearer-token", id="raw-token"),
        pytest.param("key=value&other=x", id="query-string"),
        pytest.param("<xml/>", id="xml"),
    ],
)
def test_redact_extra_rejects_non_json(extra):
    """
    The DB layer (``Connection._validate_extra``) is meant to guarantee ``extra``
    is valid JSON, so this response-model branch is defense-in-depth. If a row
    ever slips past the DB guard (direct SQL, buggy migration, legacy data), we
    must fail closed rather than return the raw value — that was the leak
    described in #63160 before the safeguard.
    """
    with pytest.raises(ValidationError):
        ConnectionResponse.model_validate(_payload(extra=extra))


class TestRedactProviderSensitiveExtra:
    """
    ``extra`` fields a provider declares sensitive are masked for that connection type only.

    The name-based masking cannot tell that ``config`` is a secret for one connection type and
    harmless for others, so the provider's own declaration decides.
    """

    @pytest.fixture(autouse=True)
    def _sensitive_fields(self, monkeypatch):
        monkeypatch.setattr(
            "airflow.api_fastapi.core_api.datamodels.connections._sensitive_extra_fields",
            lambda: {"apprise": frozenset({"config"})},
        )

    def _extra(self, conn_type, extra):
        response = ConnectionResponse.model_validate(_payload(conn_type=conn_type, extra=json.dumps(extra)))
        return json.loads(response.extra)

    def test_masks_declared_field_for_its_connection_type(self):
        extra = self._extra("apprise", {"config": '{"path": "mailto://user:pass@host"}', "tag": "alerts"})
        assert extra == {"config": "***", "tag": "alerts"}

    def test_masks_prefixed_form_of_declared_field(self):
        extra = self._extra("apprise", {"extra__apprise__config": "secret-config"})
        assert extra == {"extra__apprise__config": "***"}

    def test_leaves_same_name_alone_for_other_connection_types(self):
        extra = self._extra("generic", {"config": "not-a-secret"})
        assert extra == {"config": "not-a-secret"}

    def test_leaves_empty_declared_field_alone(self):
        assert self._extra("apprise", {"config": ""}) == {"config": ""}


def test_sensitive_extra_fields_reads_provider_declarations(monkeypatch):
    from types import SimpleNamespace

    from airflow.api_fastapi.core_api.datamodels import connections as module

    widgets = {
        "extra__apprise__config": SimpleNamespace(field_name="config", is_sensitive=True),
        "extra__apprise__tag": SimpleNamespace(field_name="tag", is_sensitive=False),
        "extra__my_type__api_secret": SimpleNamespace(field_name="api_secret", is_sensitive=True),
    }
    monkeypatch.setattr(
        "airflow.providers_manager.ProvidersManager",
        lambda: SimpleNamespace(_connection_form_widgets_from_metadata=widgets),
    )
    module._sensitive_extra_fields.cache_clear()
    try:
        assert module._sensitive_extra_fields() == {
            "apprise": frozenset({"config"}),
            "my_type": frozenset({"api_secret"}),
        }
    finally:
        module._sensitive_extra_fields.cache_clear()
