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

from airflow.utils.orm_event_handlers import _StrSubclassEncoders
from airflow.utils.state import TaskInstanceState


class _FakeConnection:
    encoding = "utf8"

    def __init__(self):
        self.encoders = {int: mock.sentinel.int_encoder}

    def string_literal(self, value: bytes) -> bytes:
        return b"'" + value + b"'"


class TestStrSubclassEncoders:
    def test_encodes_str_enum_by_value(self):
        connection = _FakeConnection()
        encoders = _StrSubclassEncoders(connection)

        encode = encoders[TaskInstanceState]

        assert encode(TaskInstanceState.FAILED, encoders) == b"'failed'"

    def test_keeps_existing_encoders(self):
        encoders = _StrSubclassEncoders(_FakeConnection())

        assert encoders[int] is mock.sentinel.int_encoder

    @pytest.mark.parametrize("key", [float, object, "not-a-type"])
    def test_leaves_other_types_to_the_driver_fallback(self, key):
        encoders = _StrSubclassEncoders(_FakeConnection())

        with pytest.raises(KeyError):
            encoders[key]
