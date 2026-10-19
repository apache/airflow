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

import copy

import pytest

from airflow.api_fastapi.common.asset_expression import redact_asset_expression

_VISIBLE = {"asset": {"uri": "s3://bucket/visible", "name": "visible", "group": "asset", "id": 1}}
_HIDDEN = {"asset": {"uri": "s3://bucket/hidden", "name": "hidden", "group": "asset", "id": 2}}
_REDACTED = {"asset": {"uri": None, "name": None, "group": "asset", "id": None, "hidden": True}}
_ALIAS = {"alias": {"name": "my_alias", "group": "asset"}}
_REF = {"asset_ref": {"name": "by_name"}}


def test_none_passes_through():
    assert redact_asset_expression(None, readable_asset_ids={1}) is None


@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        pytest.param(_VISIBLE, _VISIBLE, id="readable_leaf_unchanged"),
        pytest.param(_HIDDEN, _REDACTED, id="unreadable_leaf_redacted"),
        pytest.param(
            {"asset": {"uri": "s3://b", "name": "n", "group": "asset"}},
            _REDACTED,
            id="leaf_without_id_fails_closed",
        ),
        pytest.param(_ALIAS, _ALIAS, id="alias_untouched"),
        pytest.param(_REF, _REF, id="asset_ref_untouched"),
        pytest.param(
            {"all": [_VISIBLE, {"any": [_HIDDEN, _ALIAS]}, _HIDDEN]},
            {"all": [_VISIBLE, {"any": [_REDACTED, _ALIAS]}, _REDACTED]},
            id="nested_keeps_shape",
        ),
        pytest.param(
            {"any": ["s3://legacy-a", "s3://legacy-b"]},
            {"any": ["s3://legacy-a", "s3://legacy-b"]},
            id="legacy_string_leaves_left_for_field_coercion",
        ),
    ],
)
def test_redact(expression, expected):
    assert redact_asset_expression(expression, readable_asset_ids={1}) == expected


def test_does_not_mutate_input():
    expression = {"all": [_VISIBLE, _HIDDEN]}
    snapshot = copy.deepcopy(expression)

    redacted = redact_asset_expression(expression, readable_asset_ids={1})

    assert expression == snapshot
    assert redacted is not expression
    assert redacted["all"][0] is not expression["all"][0]
