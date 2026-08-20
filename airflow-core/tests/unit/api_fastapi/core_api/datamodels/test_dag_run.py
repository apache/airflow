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

from datetime import datetime, timezone

import pytest

from airflow.api_fastapi.core_api.datamodels.dag_run import BulkDAGRunClearBody, ClearPartitionsBody

_DATE_A = datetime(2024, 1, 1, tzinfo=timezone.utc)
_DATE_B = datetime(2024, 1, 31, tzinfo=timezone.utc)

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
