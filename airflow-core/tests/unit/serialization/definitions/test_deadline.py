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

from datetime import timedelta

import pytest

from airflow.models.variable import Variable
from airflow.serialization.definitions.deadline import SerializedVariableInterval


class TestVariableInterval:
    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("3", timedelta(seconds=3)),
            ("10", timedelta(seconds=10)),
            ("05", timedelta(seconds=5)),
            ("0", timedelta(0)),
            ("-5", timedelta(seconds=-5)),
        ],
    )
    def test_resolve_valid(self, mocker, value, expected):
        mocker.patch.object(Variable, "get", return_value=value)

        interval = SerializedVariableInterval(key="test_interval")

        assert interval.resolve() == expected

    @pytest.mark.parametrize(
        ("value", "raise_missing", "match"),
        [
            (None, True, "not found"),
            ("abc", False, "must be an integer"),
            ("", False, "must be an integer"),
        ],
    )
    def test_resolve_invalid(self, mocker, value, raise_missing, match):
        if raise_missing:
            mocker.patch.object(
                Variable,
                "get",
                side_effect=KeyError("test_interval"),
            )
        else:
            mocker.patch.object(Variable, "get", return_value=value)

        interval = SerializedVariableInterval(key="test_interval")

        with pytest.raises(ValueError, match=match):
            interval.resolve()
