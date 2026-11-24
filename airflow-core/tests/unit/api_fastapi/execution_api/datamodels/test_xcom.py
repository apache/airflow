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

from datetime import datetime

import pytest
from pydantic import JsonValue, RootModel, create_model

from airflow.api_fastapi.execution_api.datamodels.xcom import (
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)


@pytest.mark.parametrize("value", [[1, "two", None], {"a": [1.5, {"b": True}]}])
def test_xcom_response_validation_returns_decoded_value_unchanged(value):
    assert XComResponse.model_validate({"key": "return_value", "value": value}).value is value


@pytest.mark.parametrize("value", [[1, "two", None], {"a": [1.5, {"b": True}]}])
def test_xcom_sequence_index_response_validation_returns_decoded_value_unchanged(value):
    assert XComSequenceIndexResponse.model_validate(value).root is value


def test_xcom_sequence_slice_response_validation_returns_decoded_values_unchanged():
    values = [[1, "two"], {"a": [1.5]}]
    validated = XComSequenceSliceResponse.model_validate(values)
    assert all(validated.root[i] is values[i] for i in range(len(values)))


@pytest.mark.filterwarnings("error:Pydantic serializer warnings:UserWarning")
def test_xcom_response_serialization_skips_the_json_value_checks():
    # Plain JsonValue gives the same output, but warns that a datetime is none of its members.
    response = XComResponse.model_construct(key="k", value={"at": datetime(2026, 1, 1)})
    assert response.model_dump(mode="json") == {"key": "k", "value": {"at": "2026-01-01T00:00:00"}}


def test_xcom_response_publishes_the_json_value_schema():
    decoded = XComResponse.model_json_schema()["properties"]["value"]
    plain = create_model("Model", value=(JsonValue, ...)).model_json_schema()["properties"]["value"]
    assert decoded == plain


def _plain_root_model(name: str, doc: str, annotation):
    """A RootModel with the same name, docstring and root type as the model under test."""
    return type(name, (RootModel,), {"__doc__": doc, "__annotations__": {"root": annotation}})


def test_xcom_sequence_index_response_publishes_the_json_value_schema():
    plain = _plain_root_model(
        "XComSequenceIndexResponse",
        "XCom schema with minimal structure for index-based access.",
        JsonValue,
    )
    assert XComSequenceIndexResponse.model_json_schema() == plain.model_json_schema()


def test_xcom_sequence_slice_response_publishes_the_json_value_schema():
    plain = _plain_root_model(
        "XComSequenceSliceResponse",
        "XCom schema with minimal structure for slice-based access.",
        list[JsonValue],
    )
    assert XComSequenceSliceResponse.model_json_schema() == plain.model_json_schema()
