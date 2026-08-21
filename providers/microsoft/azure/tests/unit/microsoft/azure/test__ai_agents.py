#
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

from airflow.providers.microsoft.azure._ai_agents import (
    _get_agent_version,
    _get_version_status,
    _serialize_resource,
)

AGENT_NAME = "agent-123"


def test_get_version_status_raises_when_status_missing():
    with pytest.raises(ValueError, match="did not include a status"):
        _get_version_status({})


def test_get_version_status_normalizes_enum_values():
    class FakeEnum:
        value = "Active"

    assert _get_version_status({"status": FakeEnum()}) == "active"


def test_get_agent_version_raises_when_version_missing():
    with pytest.raises(ValueError, match="did not include a version"):
        _get_agent_version({})


def test_serialize_resource_uses_as_dict_serializer():
    class SdkModel:
        def as_dict(self):
            return {"name": AGENT_NAME, "versions": [{"version": "1"}]}

    assert _serialize_resource(SdkModel()) == {"name": AGENT_NAME, "versions": [{"version": "1"}]}


def test_serialize_resource_uses_model_dump_serializer():
    class PydanticModel:
        def model_dump(self):
            return {"output_text": "hello"}

    assert _serialize_resource(PydanticModel()) == {"output_text": "hello"}


def test_serialize_resource_rejects_arbitrary_objects():
    class ArbitraryObject:
        value = "class-value"

        def __init__(self):
            self.value = "instance-value"

    with pytest.raises(TypeError, match="Cannot serialize.*ArbitraryObject.*for XCom"):
        _serialize_resource(ArbitraryObject())


def test_serialize_resource_rejects_non_string_mapping_keys():
    with pytest.raises(TypeError, match="must use string keys"):
        _serialize_resource({1: "value"})
