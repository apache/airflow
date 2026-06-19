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

import warnings

import pytest

from airflow.sdk.bases.operator import BaseOperator, _warn_on_invalid_ui_color


@pytest.mark.parametrize(
    ("module", "namespace", "expected_fields"),
    [
        pytest.param("my_dags.operators", {"ui_color": "#fff"}, ["ui_color"], id="user-hex-color"),
        pytest.param("my_dags.operators", {"ui_fgcolor": "black"}, ["ui_fgcolor"], id="user-named-fgcolor"),
        pytest.param(
            "my_dags.operators",
            {"ui_color": "#fff", "ui_fgcolor": "#000"},
            ["ui_color", "ui_fgcolor"],
            id="user-both",
        ),
        pytest.param("my_dags.operators", {"ui_color": "blue.500"}, [], id="user-valid-token"),
        pytest.param("my_dags.operators", {}, [], id="user-no-override"),
        pytest.param("airflow.providers.foo", {"ui_color": "#fff"}, [], id="provider-skipped"),
        pytest.param("airflow.sdk.bases.sensor", {"ui_color": "#e6f1f2"}, [], id="core-skipped"),
    ],
)
def test_warn_on_invalid_ui_color(module, namespace, expected_fields):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_on_invalid_ui_color("MyOperator", namespace, module)

    messages = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
    assert len(messages) == len(expected_fields)
    for field in expected_fields:
        assert any(f"MyOperator.{field}" in message for message in messages)


def test_operator_subclass_warns_on_invalid_ui_color():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class BadColorOperator(BaseOperator):
            ui_color = "#e8b7e4"

            def execute(self, context):
                pass

    messages = [str(w.message) for w in caught if issubclass(w.category, UserWarning)]
    assert any("BadColorOperator.ui_color" in message for message in messages)


def test_operator_subclass_silent_on_valid_token():
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")

        class TokenColorOperator(BaseOperator):
            ui_color = "blue.500"
            ui_fgcolor = "gray.900"

            def execute(self, context):
                pass

    messages = [str(w.message) for w in caught if "Chakra color token" in str(w.message)]
    assert messages == []
