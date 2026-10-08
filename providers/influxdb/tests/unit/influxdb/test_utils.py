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

import pandas as pd
import pytest

from airflow.providers.influxdb.utils import _convert_dataframe_to_records, _first_cell_is_truthy


def test_convert_dataframe_to_records_serializes_rows_and_timestamps():
    dataframe = pd.DataFrame(
        {
            "col1": [1, 2],
            "timestamp": pd.to_datetime(["2024-01-01T00:00:00Z", "2024-01-02T03:04:05Z"], utc=True),
        }
    )

    assert _convert_dataframe_to_records(dataframe) == [
        {"col1": 1, "timestamp": "2024-01-01T00:00:00.000Z"},
        {"col1": 2, "timestamp": "2024-01-02T03:04:05.000Z"},
    ]


@pytest.mark.parametrize(
    ("dataframe", "expected"),
    [
        pytest.param(pd.DataFrame({"literal": [1]}), True, id="numeric-one"),
        pytest.param(pd.DataFrame({"count": [42]}), True, id="positive-count"),
        pytest.param(pd.DataFrame({"flag": ["ready"]}), True, id="non-empty-string"),
        pytest.param(pd.DataFrame({"literal": []}), False, id="no-rows"),
        pytest.param(pd.DataFrame(), False, id="no-columns"),
        pytest.param(pd.DataFrame({"count": [0]}), False, id="numeric-zero"),
        pytest.param(pd.DataFrame({"count": ["0"]}), False, id="string-zero"),
        pytest.param(pd.DataFrame({"value": [float("nan")]}), False, id="nan"),
        pytest.param(pd.DataFrame({"value": [None]}), False, id="none"),
        pytest.param(pd.DataFrame({"first": [0, 1], "second": [1, 1]}), False, id="first-cell-only"),
    ],
)
def test_first_cell_is_truthy(dataframe, expected):
    assert _first_cell_is_truthy(dataframe) is expected


def test_first_cell_is_truthy_rejects_non_scalar_value():
    dataframe = pd.DataFrame({"value": [[1, 2]]})

    with pytest.raises(TypeError, match="must be a scalar"):
        _first_cell_is_truthy(dataframe)
