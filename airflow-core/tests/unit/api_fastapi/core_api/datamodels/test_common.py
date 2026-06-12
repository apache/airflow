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
from pydantic import TypeAdapter, ValidationError

from airflow.api_fastapi.core_api.datamodels.common import AssetExpression, MaybeAssetExpression

# A single adapter is enough to validate/serialize the discriminated union.
_adapter: TypeAdapter[AssetExpression] = TypeAdapter(AssetExpression)
# The field type as actually wired onto the response models: tolerant of legacy shapes.
_field_adapter: TypeAdapter[MaybeAssetExpression] = TypeAdapter(MaybeAssetExpression)

# Leaf nodes as actually stored in ``DagModel.asset_expression``: ``asset`` leaves are
# enriched with the resolved ``AssetModel.id`` (see
# ``DagModelOperation.update_dag_asset_expression``), while ``alias`` and ``asset_ref``
# leaves are left as ``BaseAsset.as_expression()`` produced them.
_ASSET = {"asset": {"uri": "s3://bucket/key", "name": "my_asset", "group": "asset", "id": 7}}
_ALIAS = {"alias": {"name": "my_alias", "group": "asset"}}
_REF_BY_NAME = {"asset_ref": {"name": "by_name"}}
_REF_BY_URI = {"asset_ref": {"uri": "s3://bucket/key"}}


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param(_ASSET, id="asset"),
        pytest.param(_ALIAS, id="alias"),
        pytest.param(_REF_BY_NAME, id="asset_ref_by_name"),
        pytest.param(_REF_BY_URI, id="asset_ref_by_uri"),
        pytest.param({"any": [_ASSET, _ALIAS]}, id="any"),
        pytest.param({"all": [_ASSET, _REF_BY_NAME]}, id="all"),
        pytest.param({"all": [{"any": [_ASSET]}, _ASSET]}, id="nested"),
    ],
)
def test_asset_expression_round_trips_unchanged(expression: dict):
    """The typed model must accept and re-serialize each stored expression byte-identically."""
    validated = _adapter.validate_python(expression)
    assert _adapter.dump_python(validated, by_alias=True) == expression


def test_asset_expression_tolerates_legacy_asset_leaf_without_id():
    """
    ``asset`` leaves written by the current scheduler always carry ``id``, but a leaf persisted
    before id-enrichment may not. Such a leaf must still validate -- with ``id`` defaulting to
    ``None`` -- so a not-yet-enriched row is served instead of returning a 500. (Genuinely legacy
    pre-3.0 dataset shapes are a separate concern, covered below via ``MaybeAssetExpression``.)
    """
    validated = _adapter.validate_python({"asset": {"uri": "s3://b", "name": "n", "group": "asset"}})
    assert validated.asset.id is None


@pytest.mark.parametrize(
    "invalid",
    [
        pytest.param({}, id="empty"),
        pytest.param({"unknown": {}}, id="unknown_key"),
        pytest.param({"asset": _ASSET["asset"], "alias": _ALIAS["alias"]}, id="ambiguous_two_keys"),
        pytest.param({"asset": {"name": "a", "id": 1}}, id="asset_missing_fields"),
    ],
)
def test_asset_expression_rejects_invalid_shapes(invalid: dict):
    with pytest.raises(ValidationError):
        _adapter.validate_python(invalid)


# Shapes a released Airflow 2.x stored in ``dataset_expression`` and that the 3.0 column rename
# (``0041_rename_dataset_as_asset``) carried over verbatim into ``asset_expression``: dataset leaves
# serialized as a bare uri string, aliases as ``{"alias": "<name>"}``, composites over those strings.
_LEGACY_SHAPES = [
    pytest.param("s3://bucket/key", id="bare_uri_string"),
    pytest.param({"any": ["s3://a", "s3://b"]}, id="any_of_uri_strings"),
    pytest.param({"all": ["s3://a", "s3://b"]}, id="all_of_uri_strings"),
    pytest.param({"alias": "my_alias"}, id="alias_as_string"),
]


@pytest.mark.parametrize("legacy", _LEGACY_SHAPES)
def test_field_coerces_legacy_pre_3_0_shapes_to_none(legacy):
    """
    A row written by the pre-3.0 dataset scheduler and not yet re-parsed still holds a legacy shape
    the typed model cannot describe. The field must serve it as ``None`` -- the blank render the UI
    showed while this field was an untyped ``dict`` -- rather than 500 the whole endpoint.
    """
    assert _field_adapter.validate_python(legacy) is None


@pytest.mark.parametrize(
    "expression",
    [
        pytest.param(None, id="none"),
        pytest.param(_ASSET, id="asset"),
        pytest.param({"any": [_ASSET, _ALIAS]}, id="any"),
    ],
)
def test_field_preserves_current_shapes(expression):
    """Tolerance must not swallow valid current expressions: they pass through unchanged."""
    validated = _field_adapter.validate_python(expression)
    if expression is None:
        assert validated is None
    else:
        assert _field_adapter.dump_python(validated, by_alias=True) == expression
