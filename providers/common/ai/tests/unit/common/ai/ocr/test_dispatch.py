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

from airflow.providers.common.ai.exceptions import UnsupportedOCRProviderError
from airflow.providers.common.ai.ocr import dispatch
from airflow.providers.common.ai.ocr.base import OCRAdapter, OCRResult
from airflow.providers.common.ai.ocr.mistral import MistralOCRAdapter


class FakeAdapter(OCRAdapter):
    name = "fake"
    conn_types = frozenset({"fake_conn"})

    async def ocr(self, document, *, mime_type="application/pdf", include_images=False, **kwargs):
        return OCRResult(pages=[], model=self.model)


@pytest.fixture(autouse=True)
def _no_registered_adapters(monkeypatch):
    monkeypatch.setattr(dispatch, "_REGISTERED_ADAPTERS", {})


class TestSplitModelId:
    def test_returns_prefix_and_model(self):
        assert dispatch.split_model_id("mistral:mistral-ocr-latest") == ("mistral", "mistral-ocr-latest")

    def test_keeps_colons_of_the_model_name(self):
        assert dispatch.split_model_id("mistral:ft:model:v2") == ("mistral", "ft:model:v2")

    @pytest.mark.parametrize("model_id", [None, "", "mistral-ocr-latest", ":model", "mistral:"])
    def test_rejects_a_missing_or_empty_half(self, model_id):
        with pytest.raises(UnsupportedOCRProviderError, match="<provider>:<model>"):
            dispatch.split_model_id(model_id)


class TestGetAdapterClass:
    @pytest.mark.parametrize("conn_type", ["pydanticai", "http"])
    def test_resolves_the_built_in_mistral_adapter(self, conn_type):
        assert dispatch.get_adapter_class(conn_type, "mistral:mistral-ocr-latest") is MistralOCRAdapter

    def test_unknown_prefix_names_the_known_ones(self):
        with pytest.raises(UnsupportedOCRProviderError, match=r"'acme' has no OCR adapter.*'mistral'"):
            dispatch.get_adapter_class("http", "acme:model")

    def test_refuses_a_connection_type_the_adapter_does_not_read(self):
        with pytest.raises(UnsupportedOCRProviderError, match="does not support connection type 'postgres'"):
            dispatch.get_adapter_class("postgres", "mistral:mistral-ocr-latest")

    def test_resolves_a_registered_adapter(self):
        dispatch.register_adapter(FakeAdapter)

        assert dispatch.get_adapter_class("fake_conn", "fake:model") is FakeAdapter

    def test_a_registration_overrides_the_built_in_adapter_of_the_same_prefix(self):
        class OtherMistral(FakeAdapter):
            name = "mistral"

        dispatch.register_adapter(OtherMistral)

        assert dispatch.get_adapter_class("fake_conn", "mistral:model") is OtherMistral
