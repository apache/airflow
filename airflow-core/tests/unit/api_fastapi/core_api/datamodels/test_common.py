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

from airflow.api_fastapi.core_api.datamodels.common import AssetExpression

# A single adapter is enough to validate/serialize the discriminated union.
_adapter: TypeAdapter[AssetExpression] = TypeAdapter(AssetExpression)

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
    ``asset`` leaves written by the current scheduler always carry ``id``, but a row persisted
    before id-enrichment (or migrated from the pre-3.0 dataset format) may not. Such a leaf must
    still validate -- with ``id`` defaulting to ``None`` -- so the public endpoints that serve a
    stored expression degrade gracefully instead of returning a 500.
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
