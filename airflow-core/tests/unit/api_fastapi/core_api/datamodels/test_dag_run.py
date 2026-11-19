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
from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from airflow._shared.serialization import FORBIDDEN_XCOM_KEYS
from airflow.api_fastapi.core_api.datamodels.assets import MaterializeAssetBody
from airflow.api_fastapi.core_api.datamodels.backfills import BackfillPostBody
from airflow.api_fastapi.core_api.datamodels.dag_run import (
    BulkDAGRunClearBody,
    ClearPartitionsBody,
    TriggerDAGRunPostBody,
)

_DATE_A = datetime(2024, 1, 1, tzinfo=UTC)
_DATE_B = datetime(2024, 1, 31, tzinfo=UTC)

# Non-partition selector kwargs differ per model class.
_NON_PARTITION_KWARGS: dict[type, dict] = {
    BulkDAGRunClearBody: {"dag_runs": [{"dag_run_id": "run1", "dag_id": "dag1"}]},
    ClearPartitionsBody: {"run_id": "manual__2024-01-01"},
}


@pytest.mark.parametrize("model_cls", [BulkDAGRunClearBody, ClearPartitionsBody])
class TestHasPartitionSelectors:
    """has_partition_selectors property is driven by the shared mixin, tested for both models."""

    @pytest.mark.parametrize(
        "kwargs",
        [
            pytest.param({"partition_key": "pk"}, id="partition_key"),
            pytest.param({"partition_date_start": _DATE_A, "partition_date_end": _DATE_B}, id="date_window"),
            pytest.param({"partition_date_end": _DATE_B}, id="date_end_only"),
        ],
    )
    def test_returns_true_when_partition_selector_set(self, model_cls, kwargs):
        assert model_cls(**kwargs).has_partition_selectors is True

    def test_returns_false_when_only_non_partition_selector_provided(self, model_cls):
        body = model_cls(**_NON_PARTITION_KWARGS[model_cls])
        assert body.has_partition_selectors is False


@pytest.mark.parametrize("model_cls", [BulkDAGRunClearBody, ClearPartitionsBody])
class TestDateWindowOrder:
    """validate_partition_date_window_order is shared; verify via both model classes."""

    def test_raises_value_error_when_start_after_end(self, model_cls):
        with pytest.raises(ValueError, match="partition_date_start must be on or before partition_date_end"):
            model_cls(partition_date_start=_DATE_B, partition_date_end=_DATE_A)

    def test_accepts_equal_start_and_end(self, model_cls):
        body = model_cls(partition_date_start=_DATE_A, partition_date_end=_DATE_A)
        assert body.partition_date_start == body.partition_date_end

    def test_accepts_start_before_end(self, model_cls):
        body = model_cls(partition_date_start=_DATE_A, partition_date_end=_DATE_B)
        assert body.partition_date_start < body.partition_date_end


_CONF_BODIES = [
    pytest.param(TriggerDAGRunPostBody, "conf", {"logical_date": None}, id="trigger"),
    pytest.param(MaterializeAssetBody, "conf", {}, id="materialize"),
    pytest.param(
        BackfillPostBody,
        "dag_run_conf",
        {"dag_id": "dag1", "from_date": _DATE_A, "to_date": _DATE_B},
        id="backfill",
    ),
]


@pytest.mark.parametrize(("model_cls", "field", "base_kwargs"), _CONF_BODIES)
class TestConfReservedKeys:
    """Dag run conf rejects reserved serialization keys the way XCom values do."""

    @pytest.mark.parametrize("reserved_key", sorted(FORBIDDEN_XCOM_KEYS))
    def test_rejects_reserved_key_at_top_level(self, model_cls, field, base_kwargs, reserved_key):
        with pytest.raises(ValidationError, match="reserved serialization keys") as ctx:
            model_cls(**base_kwargs, **{field: {reserved_key: "x"}})
        assert reserved_key in str(ctx.value)

    @pytest.mark.parametrize(
        "conf",
        [
            pytest.param(
                {"a": {"__classname__": "builtins.dict", "__version__": 0, "__data__": {}}},
                id="current-nested",
            ),
            pytest.param({"a": [{"b": {"__type": "datetime", "__var": 0.0}}]}, id="legacy-nested-in-list"),
        ],
    )
    def test_rejects_nested_reserved_keys(self, model_cls, field, base_kwargs, conf):
        with pytest.raises(ValidationError, match=rf"{field}\.a.* contains reserved serialization keys"):
            model_cls(**base_kwargs, **{field: conf})

    @pytest.mark.parametrize(
        "conf",
        [
            pytest.param(None, id="none"),
            pytest.param({}, id="empty"),
            pytest.param({"env": "dev", "nested": {"type": "x", "var": [1, 2]}}, id="plain"),
            pytest.param({"a": json.dumps({"safe": "data"})}, id="benign-json-string"),
            pytest.param({"a": json.dumps({"__type": "datetime", "__var": 0.0})}, id="legacy-in-json-string"),
            pytest.param(
                {"a": json.dumps({"b": {"__classname__": "builtins.dict"}})},
                id="current-nested-in-json-string",
            ),
            pytest.param({"a": '{"not valid json'}, id="not-json"),
        ],
    )
    def test_accepts_conf_without_reserved_keys(self, model_cls, field, base_kwargs, conf):
        assert getattr(model_cls(**base_kwargs, **{field: conf}), field) == conf
